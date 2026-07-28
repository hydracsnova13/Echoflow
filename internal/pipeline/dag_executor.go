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
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if job.Status == broker.JobPaused || job.Status == broker.JobDone {
			continue
		}

		for compName, meta := range d.MemoryManager.PipelineDAG {

			if meta.ExecutionMode == "sequential" {
				canRunSeq := true
				for _, dep := range meta.DependsOn {
					if job.GlobalTasks[dep] != broker.StateDone {
						canRunSeq = false
						break
					}
				}

				if canRunSeq && job.GlobalTasks[compName] == "" {
					job.UpdateGlobalTask(compName, broker.StateRunning)
					inputPath := job.SourceFile
					if len(meta.DependsOn) > 0 {
						inputPath = filepath.Join(d.ProjectRoot, "workspace", "jobs", jobID, "out_"+meta.DependsOn[0])
					}
					if meta.Domain == "cpu" {
						go d.ExecuteCPUCommand(jobID, compName, meta, inputPath)
					}
				}
			}

			if meta.ExecutionMode == "chunked" {
				frameExtDir := filepath.Join(d.ProjectRoot, "workspace", "jobs", jobID, "out_FrameExtractor")
				entries, err := os.ReadDir(frameExtDir)
				if err != nil {
					continue
				}

				// The sorted buckets ensure FIFO sequential queueing
				var buckets []string
				for _, entry := range entries {
					if entry.IsDir() && strings.HasPrefix(entry.Name(), "bucket_") {
						buckets = append(buckets, entry.Name())
					}
				}
				sort.Strings(buckets)
				pushedNewTasks := false

				for _, chunkID := range buckets {
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

					currentState := broker.StatePending
					if job.Chunks[chunkID] != nil {
						currentState = job.Chunks[chunkID].Components[compName]
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

		if d.isJobComplete(job) {
			job.SetStatus(broker.JobDone)
			atomic.AddInt32(&d.MemoryManager.CompletedTasks, 1)
			d.MemoryManager.LogToUI(fmt.Sprintf("🏁 [DAG Engine] Job %s COMPLETED!", jobID))
			return
		}
	}
}

func (d *DAGExecutor) isJobComplete(job *broker.JobManifest) bool {
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
	if bucketCount == 0 {
		return false
	}

	job.Mu.Lock()
	defer job.Mu.Unlock()

	for compName, meta := range d.MemoryManager.PipelineDAG {
		if meta.ExecutionMode == "sequential" && job.GlobalTasks[compName] != broker.StateDone {
			return false
		}
	}
	if len(job.Chunks) != bucketCount {
		return false
	}

	for _, chunk := range job.Chunks {
		for compName, meta := range d.MemoryManager.PipelineDAG {
			if meta.ExecutionMode == "chunked" && chunk.Components[compName] != broker.StateDone {
				return false
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
