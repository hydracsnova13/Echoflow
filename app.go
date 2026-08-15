package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"ecoflow/internal/broker"
	"ecoflow/internal/pipeline"
)

type App struct {
	ctx context.Context
	MM  *broker.MemoryManager
	DAG *pipeline.DAGExecutor
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

type JobSummary struct {
	ID       string `json:"id"`
	Status   string `json:"status"`
	Progress int    `json:"progress"`
}

func (a *App) GetRecentCheckpoints() ([]JobSummary, error) {
	jobsDir := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs")
	entries, err := os.ReadDir(jobsDir)
	if err != nil {
		return []JobSummary{}, nil
	}

	var jobs []JobSummary
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "JOB-") {
			manifestPath := filepath.Join(jobsDir, entry.Name(), "manifest.json")
			fileData, err := os.ReadFile(manifestPath)
			if err != nil {
				continue
			}

			var state map[string]interface{}
			if err := json.Unmarshal(fileData, &state); err == nil {
				status := "UNKNOWN"
				if s, ok := state["status"].(string); ok {
					status = s
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

				jobs = append(jobs, JobSummary{
					ID:       entry.Name(),
					Status:   status,
					Progress: progress,
				})
			}
		}
	}

	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].ID > jobs[j].ID
	})

	return jobs, nil
}

func (a *App) SubmitJob(targetPath string, targetLang string, targetOutFormat string, minSpeakers string, maxSpeakers string, subtitleMode string) (string, error) {
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

	configPath := filepath.Join(jobDir, "job_config.json")
	configData := fmt.Sprintf(`{
		"target_language": "%s",
		"media_type": "%s",
		"output_format": "%s",
		"min_speakers": "%s",
		"max_speakers": "%s",
		"subtitle_mode": "%s"
	}`, targetLang, mediaType, targetOutFormat, minSpeakers, maxSpeakers, subtitleMode)
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

	time.Sleep(200 * time.Millisecond)

	go a.DAG.EvaluateJob(jobID)
	a.MM.EvaluateQueuesNow()
	a.MM.LogToUI(fmt.Sprintf("✅ Job %s resumed. Re-queuing incomplete tasks for execution.", jobID))
	return nil
}

var mediaServerOnce sync.Once

func (a *App) GetJobOutputPath(jobID string) map[string]string {
	// Start a lightweight local file server on port 9999 for the Wails UI to stream from
	mediaServerOnce.Do(func() {
		workspaceDir := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs")
		http.Handle("/media/", http.StripPrefix("/media/", http.FileServer(http.Dir(workspaceDir))))
		go func() {
			fmt.Println("🎬 Local Media Server started on http://localhost:9999")
			http.ListenAndServe(":9999", nil)
		}()
	})

	res := map[string]string{"Path": "", "Format": "", "Content": "", "Error": ""}
	outDir := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID, "out_MediaCompositor")

	entries, err := os.ReadDir(outDir)
	if err != nil {
		res["Error"] = "Output directory not found"
		return res
	}

	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "final_recomposed") {
			absPath := filepath.Join(outDir, e.Name())
			ext := strings.ToLower(strings.TrimPrefix(filepath.Ext(e.Name()), "."))

			// Return a standard HTTP URL instead of a blocked file:/// path
			res["Path"] = fmt.Sprintf("http://localhost:9999/media/%s/out_MediaCompositor/%s", jobID, e.Name())
			res["Format"] = ext

			// If it's a text format, read the actual text content to display in the UI
			if ext == "srt" || ext == "txt" || ext == "json" {
				bytes, _ := os.ReadFile(absPath)
				res["Content"] = string(bytes)
			}
			return res
		}
	}
	res["Error"] = "Final media not found"
	return res
}
