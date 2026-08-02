package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

				// Dynamically calculate progress based on DONE vs Total states
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

func (a *App) SubmitJob(targetPath string) (string, error) {
	targetPath = strings.Trim(strings.TrimSpace(targetPath), "\"'")

	// 🛡️ SERVER-SIDE SHIELD: Prevents users from accidentally injecting a JOB- ID
	if strings.HasPrefix(targetPath, "JOB-") {
		errMsg := fmt.Sprintf("⚠️ Invalid Input: '%s' is an existing checkpoint ID, not a media file path.", targetPath)
		a.MM.LogToUI(errMsg)
		return "", fmt.Errorf("Please provide a valid absolute file path (.mp4), not a JOB- ID")
	}

	a.MM.LogToUI(fmt.Sprintf("📥 Initiating Injection: %s", targetPath))

	if _, err := os.Stat(targetPath); os.IsNotExist(err) {
		errMsg := fmt.Sprintf("⚠️ File does not exist: %s", targetPath)
		a.MM.LogToUI(errMsg)
		return "", fmt.Errorf(errMsg)
	}

	jobID := fmt.Sprintf("JOB-%d", time.Now().Unix())
	jobDir := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID)
	os.MkdirAll(jobDir, 0755)

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

	io.Copy(dst, src)

	a.MM.Checkpoints.InitializeJob(jobID, destPath, filepath.Join(a.MM.ProjectRoot, "workspace"))
	a.MM.LogToUI(fmt.Sprintf("✅ Job %s safely created and queued!", jobID))

	go a.DAG.EvaluateJob(jobID)
	return jobID, nil
}

func (a *App) GetPipelineManifest() map[string]broker.PipelineComponent {
	return a.MM.PipelineDAG
}

// StopJob routes through CheckpointManager with Cold-Boot Recovery
func (a *App) StopJob(jobID string) error {
	a.MM.LogToUI(fmt.Sprintf("🛑 Stopping Job: %s...", jobID))
	job := a.MM.Checkpoints.GetJob(jobID)

	// 🛡️ COLD-BOOT RECOVERY: Re-hydrate job from disk if missing from RAM
	if job == nil {
		manifestPath := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID, "manifest.json")
		if _, err := os.Stat(manifestPath); err == nil {
			job = a.MM.Checkpoints.InitializeJob(jobID, "", filepath.Join(a.MM.ProjectRoot, "workspace"))
		} else {
			return fmt.Errorf("job not found in active memory or on disk")
		}
	}

	job.SetStatus(broker.JobPaused)
	a.MM.LogToUI(fmt.Sprintf("✅ Job %s successfully paused. Engine will gracefully halt.", jobID))
	return nil
}

// ResumeJob routes through CheckpointManager with Cold-Boot Recovery and Auto-Clears transient errors
func (a *App) ResumeJob(jobID string) error {
	a.MM.LogToUI(fmt.Sprintf("▶️ Resuming Job: %s...", jobID))
	job := a.MM.Checkpoints.GetJob(jobID)

	// 🛡️ COLD-BOOT RECOVERY: Re-hydrate job from disk if missing from RAM
	if job == nil {
		manifestPath := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID, "manifest.json")
		if _, err := os.Stat(manifestPath); err == nil {
			job = a.MM.Checkpoints.InitializeJob(jobID, "", filepath.Join(a.MM.ProjectRoot, "workspace"))
		} else {
			return fmt.Errorf("job not found in active memory or on disk")
		}
	}

	// 🛡️ ERROR RECOVERY: Transforms the "Resume" button into a "Retry/Recover" mechanism
	job.Mu.Lock()
	for k, v := range job.GlobalTasks {
		if v == broker.StateError {
			job.GlobalTasks[k] = broker.StatePending
		}
	}
	for chunkID, chunk := range job.Chunks {
		for k, v := range chunk.Components {
			if v == broker.StateError {
				job.Chunks[chunkID].Components[k] = broker.StatePending
			}
		}
	}
	job.Status = broker.JobRunning
	job.Mu.Unlock()

	job.Save() // Persist recovery state to manifest.json

	time.Sleep(1 * time.Second) // Let runtime garbage collect old threads

	go a.DAG.EvaluateJob(jobID)
	a.MM.LogToUI(fmt.Sprintf("✅ Job %s resumed. Retrying failed tasks and re-queuing.", jobID))
	return nil
}
