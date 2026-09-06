package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"ecoflow/internal/broker"
	"ecoflow/internal/pipeline"

	wailsRuntime "github.com/wailsapp/wails/v2/pkg/runtime"
)

type App struct {
	ctx         context.Context
	MM          *broker.MemoryManager
	DAG         *pipeline.DAGExecutor
	setupStatus string
	setupMu     sync.RWMutex
}

func NewApp(mm *broker.MemoryManager, dag *pipeline.DAGExecutor) *App {
	return &App{
		MM:  mm,
		DAG: dag,
	}
}

func (a *App) startup(ctx context.Context) {
	a.ctx = ctx
	a.MM.StartTelemetryEmitter(ctx)
}

// ==========================================
// SETUP SCRIPT RUNNER (UI-driven)
// ==========================================

func (a *App) GetMachineID() string {
	idPath := filepath.Join(a.MM.ProjectRoot, ".machine_id")
	if bytes, err := os.ReadFile(idPath); err == nil {
		return strings.TrimSpace(string(bytes))
	}
	return ""
}

func (a *App) GetSetupStatus() string {
	a.setupMu.RLock()
	defer a.setupMu.RUnlock()
	if a.setupStatus == "" {
		return "idle"
	}
	return a.setupStatus
}

func (a *App) RunSetupScript(machineID string, hfToken string) (string, error) {
	a.setupMu.Lock()
	if a.setupStatus == "running" {
		a.setupMu.Unlock()
		return "", fmt.Errorf("setup is already running")
	}
	a.setupStatus = "running"
	a.setupMu.Unlock()

	// Pre-write the machine_id file so setup_script.py detects it and skips the prompt
	machineID = strings.TrimSpace(machineID)
	if machineID != "" {
		idPath := filepath.Join(a.MM.ProjectRoot, ".machine_id")
		os.WriteFile(idPath, []byte(machineID), 0644)
	}

	go func() {
		defer func() {
			a.setupMu.Lock()
			if a.setupStatus == "running" {
				a.setupStatus = "error"
			}
			a.setupMu.Unlock()
		}()

		scriptPath := filepath.Join(a.MM.ProjectRoot, "setup_script.py")

		// Try py -3.12 first, fallback to python, then python3
		var pythonExec string
		for _, candidate := range []string{"py", "python", "python3"} {
			if _, err := exec.LookPath(candidate); err == nil {
				pythonExec = candidate
				break
			}
		}
		if pythonExec == "" {
			wailsRuntime.EventsEmit(a.ctx, "setup_log", "❌ Python not found on system PATH")
			wailsRuntime.EventsEmit(a.ctx, "setup_status", "error")
			return
		}

		var cmd *exec.Cmd
		if pythonExec == "py" {
			cmd = exec.Command(pythonExec, "-3.12", scriptPath)
		} else {
			cmd = exec.Command(pythonExec, scriptPath)
		}

		cmd.Dir = a.MM.ProjectRoot
		cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}

		// Set environment for non-interactive mode
		cmd.Env = append(os.Environ(),
			"ECOFLOW_NONINTERACTIVE=1",
			"PYTHONIOENCODING=utf-8",
			fmt.Sprintf("ECOFLOW_MACHINE_ID=%s", machineID),
		)
		if hfToken != "" {
			cmd.Env = append(cmd.Env,
				fmt.Sprintf("HF_TOKEN=%s", hfToken),
				fmt.Sprintf("HUGGING_FACE_HUB_TOKEN=%s", hfToken),
			)
		}

		// Create a pipe to read stdout+stderr
		stdoutPipe, err := cmd.StdoutPipe()
		if err != nil {
			wailsRuntime.EventsEmit(a.ctx, "setup_log", fmt.Sprintf("❌ Failed to create stdout pipe: %s", err))
			wailsRuntime.EventsEmit(a.ctx, "setup_status", "error")
			return
		}
		cmd.Stderr = cmd.Stdout // merge stderr into stdout

		wailsRuntime.EventsEmit(a.ctx, "setup_log", "🚀 Starting EcoFlow Setup...")
		wailsRuntime.EventsEmit(a.ctx, "setup_status", "running")
		wailsRuntime.EventsEmit(a.ctx, "setup_progress", 0)

		if err := cmd.Start(); err != nil {
			wailsRuntime.EventsEmit(a.ctx, "setup_log", fmt.Sprintf("❌ Failed to start setup: %s", err))
			wailsRuntime.EventsEmit(a.ctx, "setup_status", "error")
			return
		}

		// Stream output line-by-line
		scanner := bufio.NewScanner(stdoutPipe)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()

			// Parse PROGRESS markers
			if strings.HasPrefix(line, "PROGRESS:") {
				pctStr := strings.TrimPrefix(line, "PROGRESS:")
				pctStr = strings.TrimSuffix(pctStr, "%")
				if pct, err := strconv.Atoi(pctStr); err == nil {
					wailsRuntime.EventsEmit(a.ctx, "setup_progress", pct)
				}
				continue
			}

			wailsRuntime.EventsEmit(a.ctx, "setup_log", line)
		}

		if err := cmd.Wait(); err != nil {
			wailsRuntime.EventsEmit(a.ctx, "setup_log", fmt.Sprintf("❌ Setup exited with error: %s", err))
			wailsRuntime.EventsEmit(a.ctx, "setup_status", "error")
			a.setupMu.Lock()
			a.setupStatus = "error"
			a.setupMu.Unlock()
			return
		}

		wailsRuntime.EventsEmit(a.ctx, "setup_log", "🎉 Setup Complete!")
		wailsRuntime.EventsEmit(a.ctx, "setup_progress", 100)
		wailsRuntime.EventsEmit(a.ctx, "setup_status", "completed")

		a.setupMu.Lock()
		a.setupStatus = "completed"
		a.setupMu.Unlock()

		// Reload configs after setup completes
		a.MM.ReloadConfigs()
	}()

	return "Setup started", nil
}

type JobSummary struct {
	ID          string `json:"id"`
	Status      string `json:"status"`
	Progress    int    `json:"progress"`
	SourceFile  string `json:"source_file"`
	SourceTitle string `json:"source_title"`
	CreatedAt   string `json:"created_at"`
	Timestamp   int64  `json:"timestamp"`
	TotalTasks  int    `json:"total_tasks"`
	DoneTasks   int    `json:"done_tasks"`
	HasMedia    bool   `json:"has_media"`
	CanAuditASR bool   `json:"can_audit_asr"`
	CanAuditNMT bool   `json:"can_audit_nmt"`
}

func (a *App) getJobsDir() string {
	if a.MM != nil && a.MM.ProjectRoot != "" {
		cand := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs")
		if info, err := os.Stat(cand); err == nil && info.IsDir() {
			return cand
		}
	}

	if cwd, err := os.Getwd(); err == nil {
		curr := cwd
		for i := 0; i < 5; i++ {
			cand := filepath.Join(curr, "workspace", "jobs")
			if info, err := os.Stat(cand); err == nil && info.IsDir() {
				return cand
			}
			parent := filepath.Dir(curr)
			if parent == curr {
				break
			}
			curr = parent
		}
	}

	if exe, err := os.Executable(); err == nil {
		curr := filepath.Dir(exe)
		for i := 0; i < 5; i++ {
			cand := filepath.Join(curr, "workspace", "jobs")
			if info, err := os.Stat(cand); err == nil && info.IsDir() {
				return cand
			}
			parent := filepath.Dir(curr)
			if parent == curr {
				break
			}
			curr = parent
		}
	}

	return filepath.Join(a.MM.ProjectRoot, "workspace", "jobs")
}

func (a *App) GetRecentCheckpoints() ([]JobSummary, error) {
	jobsDir := a.getJobsDir()
	entries, err := os.ReadDir(jobsDir)
	if err != nil {
		return []JobSummary{}, nil
	}

	var jobs []JobSummary
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "JOB-") {
			jobDirPath := filepath.Join(jobsDir, entry.Name())
			manifestPath := filepath.Join(jobDirPath, "manifest.json")
			fileData, err := os.ReadFile(manifestPath)
			if err != nil {
				continue
			}

			info, _ := entry.Info()
			modTime := time.Now()
			if info != nil {
				modTime = info.ModTime()
			}

			var state map[string]interface{}
			if err := json.Unmarshal(fileData, &state); err == nil {
				status := "UNKNOWN"
				if s, ok := state["status"].(string); ok {
					status = s
				}

				sourceFile := ""
				if sf, ok := state["source_file"].(string); ok {
					sourceFile = sf
				}
				sourceTitle := filepath.Base(sourceFile)
				if sourceTitle == "" || sourceTitle == "." {
					sourceTitle = entry.Name()
				}

				total, done := 0, 0
				if g, ok := state["global_tasks"].(map[string]interface{}); ok {
					total += len(g)
					for _, v := range g {
						if v == "DONE" {
							done++
						}
					}
				}
				if c, ok := state["chunks"].(map[string]interface{}); ok {
					for _, chunkData := range c {
						if chunkMap, ok := chunkData.(map[string]interface{}); ok {
							if comps, ok := chunkMap["components"].(map[string]interface{}); ok {
								total += len(comps)
								for _, v := range comps {
									if v == "DONE" {
										done++
									}
								}
							}
						}
					}
				}

				progress := 0
				if total > 0 {
					progress = int((float64(done) / float64(total)) * 100)
				}

				if memJob := a.MM.Checkpoints.GetJob(entry.Name()); memJob != nil {
					memJob.Mu.Lock()
					if string(memJob.Status) != "" {
						status = string(memJob.Status)
					}
					memJob.Mu.Unlock()
				}

				if progress == 100 && (status == "RUNNING" || status == "DONE") {
					status = "COMPLETED"
					state["status"] = status
					if healedBytes, err := json.MarshalIndent(state, "", "  "); err == nil {
						os.WriteFile(manifestPath, healedBytes, 0644)
					}
					if memJob := a.MM.Checkpoints.GetJob(entry.Name()); memJob != nil {
						memJob.Mu.Lock()
						memJob.Status = broker.JobDone
						memJob.Mu.Unlock()
					}
				}

				// Check output media existence
				hasMedia := false
				mcDir := filepath.Join(jobDirPath, "out_MediaCompositor")
				if mcEntries, err := os.ReadDir(mcDir); err == nil {
					for _, me := range mcEntries {
						if strings.HasPrefix(me.Name(), "final_recomposed") {
							hasMedia = true
							break
						}
					}
				}
				if !hasMedia {
					vdDir := filepath.Join(jobDirPath, "out_VoiceDubber")
					if vdEntries, err := os.ReadDir(vdDir); err == nil && len(vdEntries) > 0 {
						hasMedia = true
					}
				}

				// Check audit readiness
				canAuditASR := false
				asrPath := filepath.Join(jobDirPath, "out_TranscriptAggregator", "master_transcript.json")
				if _, err := os.Stat(asrPath); err == nil {
					if auditDone, ok := state["audit_asr_done"].(bool); !ok || !auditDone || status == "AUDIT_ASR" {
						canAuditASR = true
					}
				}

				canAuditNMT := false
				nmtPath := filepath.Join(jobDirPath, "out_NMTTranslator", "master_translated.json")
				if _, err := os.Stat(nmtPath); err == nil {
					if auditDone, ok := state["audit_nmt_done"].(bool); !ok || !auditDone || status == "AUDIT_NMT" {
						canAuditNMT = true
					}
				}

				jobs = append(jobs, JobSummary{
					ID:          entry.Name(),
					Status:      status,
					Progress:    progress,
					SourceFile:  sourceFile,
					SourceTitle: sourceTitle,
					CreatedAt:   modTime.Format("02 Jan, 15:04"),
					Timestamp:   modTime.Unix(),
					TotalTasks:  total,
					DoneTasks:   done,
					HasMedia:    hasMedia,
					CanAuditASR: canAuditASR,
					CanAuditNMT: canAuditNMT,
				})
			}
		}
	}

	sort.Slice(jobs, func(i, j int) bool {
		if jobs[i].Timestamp != jobs[j].Timestamp {
			return jobs[i].Timestamp > jobs[j].Timestamp
		}
		return jobs[i].ID > jobs[j].ID
	})

	return jobs, nil
}

func (a *App) OpenJobFolder(jobID string) error {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return fmt.Errorf("invalid job ID")
	}
	jobsDir := a.getJobsDir()
	targetDir := filepath.Join(jobsDir, jobID)
	if info, err := os.Stat(targetDir); err != nil || !info.IsDir() {
		return fmt.Errorf("job folder not found: %s", targetDir)
	}

	cmd := exec.Command("explorer", targetDir)
	return cmd.Start()
}

func (a *App) GetJobManifest(jobID string) (string, error) {
	jobID = strings.TrimSpace(jobID)
	if jobID == "" {
		return "", fmt.Errorf("invalid job ID")
	}
	jobsDir := a.getJobsDir()
	manifestPath := filepath.Join(jobsDir, jobID, "manifest.json")
	bytes, err := os.ReadFile(manifestPath)
	if err != nil {
		return "", fmt.Errorf("checkpoint manifest not found: %w", err)
	}
	return string(bytes), nil
}


// 🛡️ FULL UI PARAMETER INJECTION: Ensures all configs from frontend are perfectly bridged to the Python daemons
func (a *App) SubmitJob(targetPath string, sourceLang string, targetLang string, targetOutFormat string, numSpeakers string, transcriptionQuality string, dubCloning string, dubSpeed string, subtitleMode string) (string, error) {
	targetPath = strings.Trim(strings.TrimSpace(targetPath), "\"'")

	if strings.HasPrefix(targetPath, "JOB-") {
		errMsg := fmt.Sprintf("⚠️ Invalid Input: '%s' is an existing checkpoint ID.", targetPath)
		a.MM.LogToUI(errMsg)
		return "", fmt.Errorf("Please provide a valid absolute file path, not a JOB- ID")
	}

	a.MM.LogToUI(fmt.Sprintf("📥 Initiating Injection: %s", targetPath))

	if _, err := os.Stat(targetPath); os.IsNotExist(err) {
		errMsg := fmt.Sprintf("⚠️ File does not exist: %s", targetPath)
		a.MM.LogToUI(errMsg)
		return "", fmt.Errorf("%s", errMsg)
	}

	jobID := fmt.Sprintf("JOB-%d", time.Now().Unix())
	jobDir := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID)
	os.MkdirAll(jobDir, 0755)

	ext := strings.ToLower(filepath.Ext(targetPath))
	mediaType := "video"
	if ext == ".wav" || ext == ".mp3" || ext == ".flac" || ext == ".m4a" || ext == ".aac" {
		mediaType = "audio"
	} else if ext == ".txt" || ext == ".json" || ext == ".srt" {
		mediaType = "text"
	}

	minSpeakers := ""
	maxSpeakers := ""
	if numSpeakers != "" && numSpeakers != "auto" {
		minSpeakers = numSpeakers
		maxSpeakers = numSpeakers
	}

	configPath := filepath.Join(jobDir, "job_config.json")
	configData := fmt.Sprintf(`{
		"source_language": "%s",
		"target_language": "%s",
		"media_type": "%s",
		"output_format": "%s",
		"num_speakers": "%s",
		"min_speakers": "%s",
		"max_speakers": "%s",
		"transcription_quality": "%s",
		"dubbing_voice_cloning": "%s",
		"dubbing_speed_mode": "%s",
		"subtitle_mode": "%s"
	}`, sourceLang, targetLang, mediaType, targetOutFormat, numSpeakers, minSpeakers, maxSpeakers, transcriptionQuality, dubCloning, dubSpeed, subtitleMode)
	os.WriteFile(configPath, []byte(configData), 0644)

	fileName := filepath.Base(targetPath)
	destPath := filepath.Join(jobDir, fileName)

	src, err := os.Open(targetPath)
	if err != nil {
		return "", err
	}
	defer src.Close()

	dst, err := os.Create(destPath)
	if err != nil {
		return "", err
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	if err != nil {
		return "", fmt.Errorf("failed to copy file: %v", err)
	}

	a.MM.Checkpoints.InitializeJob(jobID, destPath, filepath.Join(a.MM.ProjectRoot, "workspace"))
	a.MM.LogToUI(fmt.Sprintf("✅ Job %s safely created! Input: [%s] -> Output: [%s]", jobID, strings.ToUpper(mediaType), strings.ToUpper(targetOutFormat)))
	a.MM.EmitJobStatus(jobID, "RUNNING")

	go a.DAG.EvaluateJob(jobID)
	return jobID, nil
}

func (a *App) GetPipelineManifest() map[string]broker.PipelineComponent {
	return a.MM.PipelineDAG
}

func (a *App) StopJob(jobID string) error {
	jobID = strings.Trim(strings.TrimSpace(jobID), "\"'")
	a.MM.LogToUI(fmt.Sprintf("🛑 Stopping Job: %s...", jobID))
	job := a.MM.Checkpoints.GetJob(jobID)

	if job == nil {
		manifestPath := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID, "manifest.json")
		if _, err := os.Stat(manifestPath); err == nil {
			job = a.MM.Checkpoints.InitializeJob(jobID, "", filepath.Join(a.MM.ProjectRoot, "workspace"))
		} else {
			return fmt.Errorf("job not found in active memory or on disk")
		}
	}

	job.SetStatus(broker.JobPaused)
	a.MM.ClearPendingForJob(jobID)
	a.MM.EmitJobStatus(jobID, "PAUSED")
	a.MM.LogToUI(fmt.Sprintf("✅ Job %s successfully paused. Engine will gracefully halt.", jobID))
	return nil
}

func (a *App) ResumeJob(jobID string) error {
	jobID = strings.Trim(strings.TrimSpace(jobID), "\"'")
	a.MM.LogToUI(fmt.Sprintf("▶️ Resuming Job: %s...", jobID))
	job := a.MM.Checkpoints.GetJob(jobID)

	if job == nil {
		manifestPath := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID, "manifest.json")
		if _, err := os.Stat(manifestPath); err == nil {
			job = a.MM.Checkpoints.InitializeJob(jobID, "", filepath.Join(a.MM.ProjectRoot, "workspace"))
		} else {
			return fmt.Errorf("job not found in active memory or on disk")
		}
	}

	a.MM.ClearPendingForJob(jobID)
	job.ResetIncompleteTasks()
	job.Save()
	a.MM.EmitJobStatus(jobID, "RUNNING")

	time.Sleep(200 * time.Millisecond)

	go a.DAG.EvaluateJob(jobID)
	a.MM.EvaluateQueuesNow()
	a.MM.LogToUI(fmt.Sprintf("✅ Job %s resumed. Re-queuing incomplete tasks for execution.", jobID))
	return nil
}

var mediaServerOnce sync.Once

func (a *App) GetJobOutputPath(jobID string) map[string]string {
	jobsDir := a.getJobsDir()
	mediaServerOnce.Do(func() {
		http.Handle("/media/", http.StripPrefix("/media/", http.FileServer(http.Dir(jobsDir))))
		go func() {
			fmt.Println("🎬 Local Media Server started on http://localhost:9999")
			http.ListenAndServe(":9999", nil)
		}()
	})

	res := map[string]string{"Path": "", "Format": "", "Content": "", "Error": ""}

	// 1. Check out_MediaCompositor (Video / Recomposed)
	outDir := filepath.Join(jobsDir, jobID, "out_MediaCompositor")
	if entries, err := os.ReadDir(outDir); err == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "final_recomposed") {
				absPath := filepath.Join(outDir, e.Name())
				ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(e.Name()), "."))

				res["Path"] = fmt.Sprintf("http://localhost:9999/media/%s/out_MediaCompositor/%s", jobID, e.Name())
				res["Format"] = ext

				if ext == "srt" || ext == "txt" || ext == "json" {
					bytes, _ := os.ReadFile(absPath)
					res["Content"] = string(bytes)
				}
				return res
			}
		}
	}

	// 2. Check out_VoiceDubber (Dubbed Audio)
	vdDir := filepath.Join(jobsDir, jobID, "out_VoiceDubber")
	if entries, err := os.ReadDir(vdDir); err == nil {
		for _, e := range entries {
			ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(e.Name()), "."))
			if ext == "wav" || ext == "mp3" || ext == "aac" || ext == "flac" || ext == "ogg" {
				res["Path"] = fmt.Sprintf("http://localhost:9999/media/%s/out_VoiceDubber/%s", jobID, e.Name())
				res["Format"] = ext
				return res
			}
		}
	}

	res["Error"] = "Final media not found"
	return res
}

func (a *App) SaveASRTranscriptProgress(jobID string, editedJSONData string) (string, error) {
	jobID = strings.Trim(strings.TrimSpace(jobID), "\"'")
	job := a.MM.Checkpoints.GetJob(jobID)

	if job == nil {
		manifestPath := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID, "manifest.json")
		if _, err := os.Stat(manifestPath); err == nil {
			job = a.MM.Checkpoints.InitializeJob(jobID, "", filepath.Join(a.MM.ProjectRoot, "workspace"))
		} else {
			return "", fmt.Errorf("job not found in active memory or on disk")
		}
	}

	outPath := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID, "out_TranscriptAggregator", "master_transcript.json")

	if err := os.WriteFile(outPath, []byte(editedJSONData), 0644); err != nil {
		return "", fmt.Errorf("failed to save audited transcript progress: %v", err)
	}

	a.MM.LogToUI(fmt.Sprintf("💾 ASR Audit Progress saved for Job %s.", jobID))
	return "OK", nil
}

func (a *App) SaveNMTTranscriptProgress(jobID string, editedJSONData string) (string, error) {
	jobID = strings.Trim(strings.TrimSpace(jobID), "\"'")
	job := a.MM.Checkpoints.GetJob(jobID)

	if job == nil {
		manifestPath := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID, "manifest.json")
		if _, err := os.Stat(manifestPath); err == nil {
			job = a.MM.Checkpoints.InitializeJob(jobID, "", filepath.Join(a.MM.ProjectRoot, "workspace"))
		} else {
			return "", fmt.Errorf("job not found in active memory or on disk")
		}
	}

	outPath := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID, "out_NMTTranslator", "master_translated.json")

	if err := os.WriteFile(outPath, []byte(editedJSONData), 0644); err != nil {
		return "", fmt.Errorf("failed to save audited translation progress: %v", err)
	}

	a.MM.LogToUI(fmt.Sprintf("💾 NMT Audit Progress saved for Job %s.", jobID))
	return "OK", nil
}

func (a *App) ApproveASRTranscript(jobID string, editedJSONData string) (string, error) {
	jobID = strings.Trim(strings.TrimSpace(jobID), "\"'")
	job := a.MM.Checkpoints.GetJob(jobID)

	if job == nil {
		manifestPath := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID, "manifest.json")
		if _, err := os.Stat(manifestPath); err == nil {
			job = a.MM.Checkpoints.InitializeJob(jobID, "", filepath.Join(a.MM.ProjectRoot, "workspace"))
		} else {
			return "", fmt.Errorf("job not found in active memory or on disk")
		}
	}

	outPath := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID, "out_TranscriptAggregator", "master_transcript.json")

	if err := os.WriteFile(outPath, []byte(editedJSONData), 0644); err != nil {
		return "", fmt.Errorf("failed to save audited transcript: %v", err)
	}

	job.Mu.Lock()
	job.AuditASRDone = true
	job.Status = broker.JobRunning
	job.Mu.Unlock()
	job.Save()

	a.MM.EmitJobStatus(jobID, "RUNNING")
	a.MM.LogToUI(fmt.Sprintf("▶️ ASR Audit Approved. Resuming DAG for Job %s...", jobID))
	go a.DAG.EvaluateJob(jobID)

	return "OK", nil
}

func (a *App) ApproveNMTTranscript(jobID string, editedJSONData string) (string, error) {
	jobID = strings.Trim(strings.TrimSpace(jobID), "\"'")
	job := a.MM.Checkpoints.GetJob(jobID)

	if job == nil {
		manifestPath := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID, "manifest.json")
		if _, err := os.Stat(manifestPath); err == nil {
			job = a.MM.Checkpoints.InitializeJob(jobID, "", filepath.Join(a.MM.ProjectRoot, "workspace"))
		} else {
			return "", fmt.Errorf("job not found in active memory or on disk")
		}
	}

	outPath := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID, "out_NMTTranslator", "master_translated.json")

	if err := os.WriteFile(outPath, []byte(editedJSONData), 0644); err != nil {
		return "", fmt.Errorf("failed to save audited translation: %v", err)
	}

	job.Mu.Lock()
	job.AuditNMTDone = true
	job.Status = broker.JobRunning
	job.Mu.Unlock()
	job.Save()

	a.MM.EmitJobStatus(jobID, "RUNNING")
	a.MM.LogToUI(fmt.Sprintf("▶️ NMT Audit Approved. Resuming DAG for Job %s...", jobID))
	go a.DAG.EvaluateJob(jobID)

	return "OK", nil
}

// ==========================================
// DYNAMIC DICTIONARY SHARD MANAGEMENT
// ==========================================

func (a *App) getMachineID() string {
	idPath := filepath.Join(a.MM.ProjectRoot, ".machine_id")
	if bytes, err := os.ReadFile(idPath); err == nil {
		return strings.TrimSpace(string(bytes))
	}
	randomBytes := make([]byte, 4)
	rand.Read(randomBytes)
	newID := hex.EncodeToString(randomBytes)
	os.WriteFile(idPath, []byte(newID), 0644)
	return newID
}

type ShardMeta struct {
	MachineID   string `json:"machine_id"`
	LastUpdated int64  `json:"last_updated"`
}

type DictShard struct {
	Meta                   ShardMeta         `json:"_meta"`
	AsrCorrections         map[string]string `json:"asr_corrections"`
	DomainTerms            map[string]string `json:"domain_terms"`
	AsrStemPatterns        []interface{}     `json:"asr_stem_patterns"`
	SpokenEnglishSmoothing []interface{}     `json:"spoken_english_smoothing"`
	SpokenHindiSmoothing   []interface{}     `json:"spoken_hindi_smoothing"`
	SpokenMarathiSmoothing []interface{}     `json:"spoken_marathi_smoothing"`
}

func (a *App) GetSyncStatus() string {
	return a.MM.SyncManager.GetStatus()
}

func (a *App) TriggerManualSync() string {
	go a.SyncLocalShard()
	return "Sync Initiated"
}

func (a *App) SyncLocalShard() (string, error) {
	machineID := a.getMachineID()
	return a.MM.SyncManager.SyncLocal(machineID, a.MM.LogToUI)
}

func (a *App) SyncGlobalRepo() (string, error) {
	return a.MM.SyncManager.SyncGlobal(a.MM.LogToUI)
}

func (a *App) UpdateDomainDictionary() (string, error) {
	_, err := a.MM.SyncManager.UpdateDomainDictionary(a.MM.LogToUI)
	if err != nil {
		return "", err
	}
	return a.GetGlobalDictionary()
}

// ==========================================
// 📊 TELEMETRY & LOG STORAGE MANAGEMENT
// ==========================================

func (a *App) GetLogStorageStats() (broker.LogStorageStats, error) {
	if a.MM.TelemetryLogger == nil {
		return broker.LogStorageStats{}, fmt.Errorf("telemetry logger not initialized")
	}
	return a.MM.TelemetryLogger.GetStorageStats()
}

func (a *App) DeleteLogsByFilter(periodValue int, periodUnit string, clearAll bool) (broker.DeleteResult, error) {
	if a.MM.TelemetryLogger == nil {
		return broker.DeleteResult{}, fmt.Errorf("telemetry logger not initialized")
	}

	if clearAll || periodValue <= 0 {
		return a.MM.TelemetryLogger.ClearAllLogs()
	}

	now := time.Now()
	var cutoff time.Time

	switch strings.ToLower(strings.TrimSpace(periodUnit)) {
	case "years", "year", "y":
		cutoff = now.AddDate(-periodValue, 0, 0)
	case "months", "month", "m":
		cutoff = now.AddDate(0, -periodValue, 0)
	case "days", "day", "d":
		cutoff = now.AddDate(0, 0, -periodValue)
	default:
		cutoff = now.AddDate(0, 0, -periodValue)
	}

	return a.MM.TelemetryLogger.DeleteLogsOlderThan(cutoff)
}

func (a *App) GetRecentTelemetryLogs(limit int) ([]string, error) {
	if a.MM.TelemetryLogger == nil {
		return []string{}, fmt.Errorf("telemetry logger not initialized")
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	return a.MM.TelemetryLogger.GetRecentLogs(limit)
}

func deduplicateShardMap(m map[string]string) map[string]string {
	result := make(map[string]string)
	lowerToKey := make(map[string]string)
	for k, v := range m {
		kTrimmed := strings.TrimSpace(k)
		vTrimmed := strings.TrimSpace(v)
		if kTrimmed == "" {
			continue
		}
		kLower := strings.ToLower(kTrimmed)
		if oldKey, exists := lowerToKey[kLower]; exists {
			delete(result, oldKey)
		}
		lowerToKey[kLower] = kTrimmed
		result[kTrimmed] = vTrimmed
	}
	return result
}

func deduplicateShardPatterns(list []interface{}) []interface{} {
	seen := make(map[string]bool)
	var result []interface{}
	for _, item := range list {
		if itemMap, ok := item.(map[string]interface{}); ok {
			pattern, _ := itemMap["pattern"].(string)
			pTrimmed := strings.TrimSpace(pattern)
			if pTrimmed == "" {
				continue
			}
			pKey := strings.ToLower(pTrimmed)
			if !seen[pKey] {
				seen[pKey] = true
				result = append(result, item)
			}
		} else {
			result = append(result, item)
		}
	}
	return result
}

func (a *App) GetGlobalDictionary() (string, error) {
	basePath := filepath.Join(a.MM.ProjectRoot, "pipeline", "config", "domain_dictionary.json")
	var compiled DictShard

	baseBytes, err := os.ReadFile(basePath)
	if err == nil {
		json.Unmarshal(baseBytes, &compiled)
	}

	if compiled.AsrCorrections == nil {
		compiled.AsrCorrections = make(map[string]string)
	}
	if compiled.DomainTerms == nil {
		compiled.DomainTerms = make(map[string]string)
	}

	shardsDir := filepath.Join(a.MM.ProjectRoot, "pipeline", "config", "shards")
	entries, _ := os.ReadDir(shardsDir)

	var shards []DictShard
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".json") {
			shardBytes, err := os.ReadFile(filepath.Join(shardsDir, entry.Name()))
			if err == nil {
				var shard DictShard
				if json.Unmarshal(shardBytes, &shard) == nil {
					shards = append(shards, shard)
				}
			}
		}
	}

	sort.Slice(shards, func(i, j int) bool {
		return shards[i].Meta.LastUpdated < shards[j].Meta.LastUpdated
	})

	for _, shard := range shards {
		for k, v := range shard.AsrCorrections {
			compiled.AsrCorrections[k] = v
		}
		for k, v := range shard.DomainTerms {
			compiled.DomainTerms[k] = v
		}
		if len(shard.AsrStemPatterns) > 0 {
			compiled.AsrStemPatterns = append(compiled.AsrStemPatterns, shard.AsrStemPatterns...)
		}
		if len(shard.SpokenEnglishSmoothing) > 0 {
			compiled.SpokenEnglishSmoothing = append(compiled.SpokenEnglishSmoothing, shard.SpokenEnglishSmoothing...)
		}
		if len(shard.SpokenHindiSmoothing) > 0 {
			compiled.SpokenHindiSmoothing = append(compiled.SpokenHindiSmoothing, shard.SpokenHindiSmoothing...)
		}
		if len(shard.SpokenMarathiSmoothing) > 0 {
			compiled.SpokenMarathiSmoothing = append(compiled.SpokenMarathiSmoothing, shard.SpokenMarathiSmoothing...)
		}
	}

	res, _ := json.Marshal(compiled)
	return string(res), nil
}

func (a *App) GetMyDictionaryShard() (string, error) {
	machineID := a.getMachineID()
	shardPath := filepath.Join(a.MM.ProjectRoot, "pipeline", "config", "shards", fmt.Sprintf("dict_%s.json", machineID))

	bytes, err := os.ReadFile(shardPath)
	if err != nil {
		scaffold := fmt.Sprintf(`{"_meta":{"machine_id":"%s","last_updated":0},"asr_corrections":{},"domain_terms":{},"asr_stem_patterns":[],"spoken_english_smoothing":[],"spoken_hindi_smoothing":[],"spoken_marathi_smoothing":[]}`, machineID)
		return scaffold, nil
	}
	return string(bytes), nil
}

func (a *App) SaveMyDictionaryShard(dictJSON string) (string, error) {
	var shard DictShard
	if err := json.Unmarshal([]byte(dictJSON), &shard); err != nil {
		return "", fmt.Errorf("invalid JSON syntax: %v", err)
	}

	machineID := a.getMachineID()
	shard.Meta = ShardMeta{
		MachineID:   machineID,
		LastUpdated: time.Now().Unix(),
	}

	// Conduct deduplication on shard entries
	shard.AsrCorrections = deduplicateShardMap(shard.AsrCorrections)
	shard.DomainTerms = deduplicateShardMap(shard.DomainTerms)
	shard.AsrStemPatterns = deduplicateShardPatterns(shard.AsrStemPatterns)
	shard.SpokenEnglishSmoothing = deduplicateShardPatterns(shard.SpokenEnglishSmoothing)
	shard.SpokenHindiSmoothing = deduplicateShardPatterns(shard.SpokenHindiSmoothing)
	shard.SpokenMarathiSmoothing = deduplicateShardPatterns(shard.SpokenMarathiSmoothing)

	shardsDir := filepath.Join(a.MM.ProjectRoot, "pipeline", "config", "shards")
	os.MkdirAll(shardsDir, 0755)

	shardPath := filepath.Join(shardsDir, fmt.Sprintf("dict_%s.json", machineID))

	formattedBytes, _ := json.MarshalIndent(shard, "", "  ")
	if err := os.WriteFile(shardPath, formattedBytes, 0644); err != nil {
		return "", fmt.Errorf("failed to write local shard: %v", err)
	}

	// NOTE: Per specification, saving locally does NOT modify domain_dictionary.json.
	// domain_dictionary.json is only updated when the user clicks 'Sync Local' or 'Update Domain Dictionary'.
	a.MM.LogToUI("📖 Local Contribution saved to disk with deduplication (Domain Dictionary untouched until sync).")
	return "OK", nil
}

func (a *App) GetJobCandidateTerms(jobID string) (string, error) {
	jobID = strings.Trim(strings.TrimSpace(jobID), "\"'")
	candidatePath := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID, "out_NMTTranslator", "candidate_terms.json")
	if _, err := os.Stat(candidatePath); os.IsNotExist(err) {
		return "[]", nil
	}
	bytes, err := os.ReadFile(candidatePath)
	if err != nil {
		return "[]", nil
	}
	return string(bytes), nil
}

func (a *App) GetAllPendingCandidateTerms() (string, error) {
	compiledJSON, _ := a.GetGlobalDictionary()
	var compiled DictShard
	json.Unmarshal([]byte(compiledJSON), &compiled)

	existingTerms := make(map[string]bool)
	for k := range compiled.AsrCorrections {
		existingTerms[strings.TrimSpace(k)] = true
	}
	for k := range compiled.DomainTerms {
		existingTerms[strings.TrimSpace(k)] = true
	}

	jobsDir := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs")
	entries, err := os.ReadDir(jobsDir)
	if err != nil {
		return "[]", nil
	}

	var allCandidates []map[string]interface{}
	seenTerms := make(map[string]bool)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		candFile := filepath.Join(jobsDir, entry.Name(), "out_NMTTranslator", "candidate_terms.json")
		if bytes, err := os.ReadFile(candFile); err == nil {
			var list []map[string]interface{}
			if err := json.Unmarshal(bytes, &list); err == nil {
				for _, item := range list {
					orig, _ := item["original"].(string)
					origTrimmed := strings.TrimSpace(orig)
					if origTrimmed != "" && !existingTerms[origTrimmed] && !seenTerms[origTrimmed] {
						seenTerms[origTrimmed] = true
						item["job_id"] = entry.Name()
						allCandidates = append(allCandidates, item)
					}
				}
			}
		}
	}

	resBytes, err := json.Marshal(allCandidates)
	if err != nil {
		return "[]", nil
	}
	return string(resBytes), nil
}

type ApprovedTermCandidate struct {
	Original    string `json:"original"`
	Replacement string `json:"replacement"`
	Type        string `json:"type"`
}

func (a *App) ApproveCandidateTerms(termsJSON string) (string, error) {
	var candidates []ApprovedTermCandidate
	if err := json.Unmarshal([]byte(termsJSON), &candidates); err != nil {
		return "", fmt.Errorf("invalid terms JSON: %v", err)
	}

	if len(candidates) == 0 {
		return "No terms provided", nil
	}

	shardJSON, _ := a.GetMyDictionaryShard()
	var localShard DictShard
	json.Unmarshal([]byte(shardJSON), &localShard)

	if localShard.AsrCorrections == nil {
		localShard.AsrCorrections = make(map[string]string)
	}
	if localShard.DomainTerms == nil {
		localShard.DomainTerms = make(map[string]string)
	}

	approvedKeys := make(map[string]bool)
	addedCount := 0
	for _, term := range candidates {
		orig := strings.TrimSpace(term.Original)
		repl := strings.TrimSpace(term.Replacement)
		if orig == "" || repl == "" {
			continue
		}

		if term.Type == "domain_terms" {
			localShard.DomainTerms[orig] = repl
		} else {
			localShard.AsrCorrections[orig] = repl
		}
		approvedKeys[orig] = true
		addedCount++
	}

	updatedShardJSON, _ := json.Marshal(localShard)
	a.SaveMyDictionaryShard(string(updatedShardJSON))

	jobsDir := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs")
	if entries, err := os.ReadDir(jobsDir); err == nil {
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			candFile := filepath.Join(jobsDir, entry.Name(), "out_NMTTranslator", "candidate_terms.json")
			if cBytes, err := os.ReadFile(candFile); err == nil {
				var list []map[string]interface{}
				if err := json.Unmarshal(cBytes, &list); err == nil {
					var remaining []map[string]interface{}
					for _, item := range list {
						orig, _ := item["original"].(string)
						if !approvedKeys[strings.TrimSpace(orig)] {
							remaining = append(remaining, item)
						}
					}
					if newCBytes, err := json.MarshalIndent(remaining, "", "  "); err == nil {
						os.WriteFile(candFile, newCBytes, 0644)
					}
				}
			}
		}
	}

	a.MM.LogToUI(fmt.Sprintf("✨ Approved %d technical term(s) and saved to local shard!", addedCount))
	return fmt.Sprintf("%d terms approved", addedCount), nil
}

func (a *App) GetAuditData(jobID string, auditType string) (string, error) {
	jobID = strings.Trim(strings.TrimSpace(jobID), "\"'")

	folder := "out_TranscriptAggregator"
	file := "master_transcript.json"

	if auditType == "NMT" {
		folder = "out_NMTTranslator"
		file = "master_translated.json"
	}

	path := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID, folder, file)

	bytes, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed to read audit file: %v", err)
	}

	return string(bytes), nil
}
