package pipeline

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"ecoflow/internal/broker"
)

type DAGExecutor struct {
	MemoryManager     *broker.MemoryManager
	CheckpointManager *broker.CheckpointManager
	PythonExec        string
	ProjectRoot       string
}

func NewDAGExecutor(mm *broker.MemoryManager, cm *broker.CheckpointManager, pyExec, root string) *DAGExecutor {
	return &DAGExecutor{
		MemoryManager:     mm,
		CheckpointManager: cm,
		PythonExec:        pyExec,
		ProjectRoot:       root,
	}
}

func (d *DAGExecutor) EvaluateJob(jobID string) {
	job := d.CheckpointManager.GetJob(jobID)
	if job == nil {
		return
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	// 🛡️ THE FIX: Transient Retry Tracker
	// This map lives only as long as the job is running. It tracks how many times
	// a specific task has failed to prevent infinite crash loops.
	retryCounts := make(map[string]int)

	for range ticker.C {
		if job.Status == broker.JobPaused {
			d.MemoryManager.LogToUI(fmt.Sprintf("⏸️ [DAG Engine] Job %s thread gracefully suspended.", jobID))
			return
		}
		if job.Status == broker.JobDone {
			return
		}

		hasFatalError := false

		for compName, meta := range d.MemoryManager.PipelineDAG {

			// ==========================================
			// 1. SEQUENTIAL TASK EXECUTION
			// ==========================================
			if meta.ExecutionMode == "sequential" {

				// 🛡️ Auto-Recovery for CPU Tasks
				if job.GlobalTasks[compName] == broker.StateError {
					retryKey := compName
					if retryCounts[retryKey] < 3 {
						retryCounts[retryKey]++
						d.MemoryManager.LogToUI(fmt.Sprintf("🔄 [Auto-Recovery] Retrying crashed CPU task %s (Attempt %d/3)", compName, retryCounts[retryKey]))
						job.UpdateGlobalTask(compName, "") // Clear state to instantly re-trigger
					} else {
						hasFatalError = true
					}
				}

				canRunSeq := true
				for _, dep := range meta.DependsOn {
					depMeta := d.MemoryManager.PipelineDAG[dep]

					if depMeta.ExecutionMode == "sequential" {
						if job.GlobalTasks[dep] != broker.StateDone {
							canRunSeq = false
							break
						}
					} else if depMeta.ExecutionMode == "chunked" {
						frameExtDir := filepath.Join(d.ProjectRoot, "workspace", "jobs", jobID, "out_FrameExtractor")
						entries, err := os.ReadDir(frameExtDir)
						if err != nil {
							canRunSeq = false
							break
						}

						bucketCount := 0
						for _, entry := range entries {
							if entry.IsDir() && strings.HasPrefix(entry.Name(), "bucket_") {
								bucketCount++
							}
						}

						if bucketCount == 0 || len(job.Chunks) != bucketCount {
							canRunSeq = false
							break
						}

						for _, chunk := range job.Chunks {
							if chunk.Components[dep] != broker.StateDone {
								canRunSeq = false
								break
							}
						}
					}
				}

				if canRunSeq && job.GlobalTasks[compName] == "" {
					job.UpdateGlobalTask(compName, broker.StateRunning)

					inputPath := job.SourceFile
					if len(meta.DependsOn) > 0 {
						depMeta := d.MemoryManager.PipelineDAG[meta.DependsOn[0]]
						if depMeta.ExecutionMode == "chunked" {
							inputPath = filepath.Join(d.ProjectRoot, "workspace", "jobs", jobID)
						} else {
							inputPath = filepath.Join(d.ProjectRoot, "workspace", "jobs", jobID, "out_"+meta.DependsOn[0])
						}
					}

					if meta.Domain == "cpu" {
						go d.ExecuteCPUCommand(jobID, compName, meta, inputPath)
					}
				}
			}

			// ==========================================
			// 2. CHUNKED DAEMON EXECUTION
			// ==========================================
			if meta.ExecutionMode == "chunked" {
				frameExtDir := filepath.Join(d.ProjectRoot, "workspace", "jobs", jobID, "out_FrameExtractor")
				entries, err := os.ReadDir(frameExtDir)
				if err != nil {
					continue
				}

				var buckets []string
				for _, entry := range entries {
					if entry.IsDir() && strings.HasPrefix(entry.Name(), "bucket_") {
						buckets = append(buckets, entry.Name())
					}
				}
				sort.Strings(buckets)
				pushedNewTasks := false

				for _, chunkID := range buckets {
					currentState := broker.StatePending
					if job.Chunks[chunkID] != nil {
						currentState = job.Chunks[chunkID].Components[compName]
					}

					// 🛡️ Auto-Recovery for RAM Daemons (Race Condition Fix)
					if currentState == broker.StateError {
						retryKey := chunkID + "_" + compName
						if retryCounts[retryKey] < 3 {
							retryCounts[retryKey]++
							d.MemoryManager.LogToUI(fmt.Sprintf("🔄 [Auto-Recovery] Re-queuing failed chunk %s for %s (Attempt %d/3)", chunkID, compName, retryCounts[retryKey]))

							// Clear the error state in memory and on disk
							currentState = broker.StatePending
							job.UpdateChunkState(chunkID, compName, broker.StatePending)
						} else {
							hasFatalError = true
						}
					}

					if currentState == broker.StateDone || currentState == broker.StateRunning || currentState == broker.StateQueued {
						continue
					}

					chunkCanRun := true
					for _, dep := range meta.DependsOn {
						depMeta := d.MemoryManager.PipelineDAG[dep]
						if depMeta.ExecutionMode == "sequential" {
							if job.GlobalTasks[dep] != broker.StateDone && job.GlobalTasks[dep] != broker.StateRunning {
								chunkCanRun = false
							}
						} else if depMeta.ExecutionMode == "chunked" {
							if job.Chunks[chunkID] == nil || job.Chunks[chunkID].Components[dep] != broker.StateDone {
								chunkCanRun = false
							}
						}
					}

					if !chunkCanRun {
						continue
					}

					if currentState == "" || currentState == broker.StatePending {
						inputPath := filepath.Join(frameExtDir, chunkID)
						job.UpdateChunkState(chunkID, compName, broker.StateQueued)

						d.MemoryManager.PushBucket(broker.BucketTask{
							JobID:     jobID,
							ChunkID:   chunkID,
							Component: compName,
							InputData: inputPath,
						})
						pushedNewTasks = true
					}
				}

				if pushedNewTasks {
					d.MemoryManager.EvaluateQueuesNow()
				}
			}
		}

		// 🛡️ Final Strike Protocol
		// If any chunk fails 3 times, the system assumes a hard crash and pauses
		if hasFatalError {
			job.SetStatus(broker.JobPaused)
			d.MemoryManager.LogToUI(fmt.Sprintf("⚠️ [DAG Engine] Job %s suspended after exceeding retry limits. Manual resume required.", jobID))
			return
		}

		if d.isJobComplete(job) {
			job.SetStatus(broker.JobDone)
			atomic.AddInt32(&d.MemoryManager.CompletedTasks, 1)
			d.MemoryManager.LogToUI(fmt.Sprintf("🏁 [DAG Engine] Job %s COMPLETED! All branches synchronized.", jobID))
			return
		}
	}
}

func (d *DAGExecutor) isJobComplete(job *broker.JobManifest) bool {
	job.Mu.Lock()
	defer job.Mu.Unlock()

	for compName, meta := range d.MemoryManager.PipelineDAG {
		if meta.ExecutionMode == "sequential" && job.GlobalTasks[compName] != broker.StateDone {
			return false
		}
	}

	frameExtDir := filepath.Join(d.ProjectRoot, "workspace", "jobs", job.JobID, "out_FrameExtractor")
	entries, err := os.ReadDir(frameExtDir)
	if err != nil {
		return false
	}

	bucketCount := 0
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "bucket_") {
			bucketCount++
		}
	}

	if bucketCount == 0 || len(job.Chunks) != bucketCount {
		return false
	}

	for _, chunk := range job.Chunks {
		for compName, meta := range d.MemoryManager.PipelineDAG {
			if meta.ExecutionMode == "chunked" {
				if state, exists := chunk.Components[compName]; !exists || state != broker.StateDone {
					return false
				}
			}
		}
	}

	return true
}

func (d *DAGExecutor) ExecuteCPUCommand(jobID string, compName string, meta broker.PipelineComponent, inputFile string) {
	job := d.CheckpointManager.GetJob(jobID)
	startMsg := fmt.Sprintf("⚙️ [DAG Engine] Starting CPU Task: %s", compName)
	d.MemoryManager.LogToUI(startMsg)

	jobDir := filepath.Join(d.ProjectRoot, "workspace", "jobs", jobID)
	outputDir := filepath.Join(jobDir, "out_"+compName)
	scriptPath := filepath.Join(d.ProjectRoot, meta.Script)

	cmd := exec.Command(d.PythonExec, scriptPath, inputFile, outputDir)
	cmd.Env = append(os.Environ(), "PYTHONIOENCODING=utf-8")
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}

	out, err := cmd.CombinedOutput()
	pythonLogs := strings.TrimSpace(string(out))

	if err != nil {
		d.MemoryManager.LogToUI(fmt.Sprintf("❌ %s crashed! Error: %v | Logs: %s", compName, err, pythonLogs))
		job.UpdateGlobalTask(compName, broker.StateError)
		return
	}

	job.UpdateGlobalTask(compName, broker.StateDone)
	d.MemoryManager.LogToUI(fmt.Sprintf("✅ %s finished. Output: %s", compName, pythonLogs))
}
