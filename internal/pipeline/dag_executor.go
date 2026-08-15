package pipeline

import (
	"bytes"
	"encoding/json"
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
	ProjectRoot       string
}

func NewDAGExecutor(mm *broker.MemoryManager, cm *broker.CheckpointManager, root string) *DAGExecutor {
	return &DAGExecutor{
		MemoryManager:     mm,
		CheckpointManager: cm,
		ProjectRoot:       root,
	}
}

func (d *DAGExecutor) getChunkSourceDir(jobID, compName string) string {
	curr := compName
	for {
		meta := d.MemoryManager.PipelineDAG[curr]
		if meta.ExecutionMode == "sequential" {
			return filepath.Join(d.ProjectRoot, "workspace", "jobs", jobID, "out_"+curr)
		}
		if len(meta.DependsOn) == 0 {
			break
		}
		curr = meta.DependsOn[0]
	}
	return ""
}

func (d *DAGExecutor) getJobConfig(jobID string) (mediaType string, outputFormat string) {
	configPath := filepath.Join(d.ProjectRoot, "workspace", "jobs", jobID, "job_config.json")
	mediaType = "video"
	outputFormat = "video"
	file, err := os.ReadFile(configPath)
	if err != nil {
		return
	}
	var config map[string]interface{}
	if err := json.Unmarshal(file, &config); err == nil {
		if mt, ok := config["media_type"].(string); ok {
			mediaType = mt
		}
		if of, ok := config["output_format"].(string); ok {
			outputFormat = of
		}
	}
	return
}

func isBypassed(mediaType, outputFormat, compName string) bool {
	isVideoNode := compName == "FrameExtractor" || compName == "FaceDetector" || compName == "FacialLandmarker" || compName == "MouthIsolator"
	isAudioASRNode := compName == "AudioChunker" || compName == "WhisperTranscriber" || compName == "SpeakerDiarizer" || compName == "TranscriptAggregator"

	if mediaType == "text" {
		if isVideoNode || isAudioASRNode {
			return true
		}
	}
	if mediaType == "audio" || outputFormat == "audio" || outputFormat == "text" {
		if isVideoNode {
			return true
		}
	}
	if outputFormat == "text" {
		if compName == "VoiceDubber" {
			return true
		}
	}
	return false
}

func (d *DAGExecutor) isDependencySatisfied(job *broker.JobManifest, compName, mediaType, outputFormat string) bool {
	if isBypassed(mediaType, outputFormat, compName) {
		meta := d.MemoryManager.PipelineDAG[compName]
		if len(meta.DependsOn) == 0 {
			return true
		}
		for _, dep := range meta.DependsOn {
			if !d.isDependencySatisfied(job, dep, mediaType, outputFormat) {
				return false
			}
		}
		return true
	}

	meta := d.MemoryManager.PipelineDAG[compName]
	if meta.ExecutionMode == "sequential" {
		return job.GlobalTasks[compName] == broker.StateDone
	} else if meta.ExecutionMode == "chunked" {
		chunkDir := d.getChunkSourceDir(job.JobID, compName)
		entries, err := os.ReadDir(chunkDir)
		if err != nil {
			return false
		}
		bucketCount := 0
		for _, entry := range entries {
			if entry.IsDir() && strings.HasPrefix(entry.Name(), "bucket_") {
				bucketCount++
				chunkID := entry.Name()
				if job.Chunks[chunkID] == nil || job.Chunks[chunkID].Components[compName] != broker.StateDone {
					return false
				}
			}
		}
		return bucketCount > 0
	}
	return false
}

func (d *DAGExecutor) getActiveInputPath(jobID, compName, mediaType, outputFormat string) string {
	meta := d.MemoryManager.PipelineDAG[compName]

	if len(meta.DependsOn) == 0 {
		return d.CheckpointManager.GetJob(jobID).SourceFile
	}

	currDep := meta.DependsOn[0]

	for isBypassed(mediaType, outputFormat, currDep) {
		depMeta := d.MemoryManager.PipelineDAG[currDep]
		if len(depMeta.DependsOn) == 0 {
			return d.CheckpointManager.GetJob(jobID).SourceFile
		}
		currDep = depMeta.DependsOn[0]
	}

	depMeta := d.MemoryManager.PipelineDAG[currDep]
	if depMeta.ExecutionMode == "chunked" {
		return filepath.Join(d.ProjectRoot, "workspace", "jobs", jobID)
	}
	return filepath.Join(d.ProjectRoot, "workspace", "jobs", jobID, "out_"+currDep)
}

func (d *DAGExecutor) EvaluateJob(jobID string) {
	job := d.CheckpointManager.GetJob(jobID)
	if job == nil {
		return
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

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
		mediaType, outputFormat := d.getJobConfig(jobID)

		for compName, meta := range d.MemoryManager.PipelineDAG {

			if isBypassed(mediaType, outputFormat, compName) {
				if meta.ExecutionMode == "sequential" && job.GlobalTasks[compName] != broker.StateDone {
					job.UpdateGlobalTask(compName, broker.StateDone)
				} else if meta.ExecutionMode == "chunked" {
					chunkDir := d.getChunkSourceDir(jobID, compName)
					entries, _ := os.ReadDir(chunkDir)
					for _, entry := range entries {
						if entry.IsDir() && strings.HasPrefix(entry.Name(), "bucket_") {
							chunkID := entry.Name()
							job.UpdateChunkState(chunkID, compName, broker.StateDone)
						}
					}
				}
				continue
			}

			if meta.ExecutionMode == "sequential" {

				if job.GlobalTasks[compName] == broker.StateError {
					retryKey := compName
					if retryCounts[retryKey] < 3 {
						retryCounts[retryKey]++
						d.MemoryManager.LogToUI(fmt.Sprintf("🔄 [Auto-Recovery] Retrying crashed CPU task %s (Attempt %d/3)", compName, retryCounts[retryKey]))
						job.UpdateGlobalTask(compName, "")
					} else {
						hasFatalError = true
					}
				}

				canRunSeq := true
				for _, dep := range meta.DependsOn {
					if !d.isDependencySatisfied(job, dep, mediaType, outputFormat) {
						canRunSeq = false
						break
					}
				}

				if canRunSeq && job.GlobalTasks[compName] == "" {
					job.UpdateGlobalTask(compName, broker.StateRunning)
					inputPath := d.getActiveInputPath(jobID, compName, mediaType, outputFormat)

					if meta.Domain == "cpu" {
						go d.ExecuteCPUCommand(jobID, compName, meta, inputPath)
					}
				}
			}

			if meta.ExecutionMode == "chunked" {

				upstreamActive := true
				if len(meta.DependsOn) > 0 {
					for _, dep := range meta.DependsOn {
						if !d.isDependencySatisfied(job, dep, mediaType, outputFormat) && job.GlobalTasks[dep] != broker.StateRunning {
							upstreamActive = false
							break
						}
					}
				}

				if len(meta.DependsOn) > 0 && !upstreamActive {
					continue
				}

				chunkDir := d.getChunkSourceDir(jobID, compName)
				entries, err := os.ReadDir(chunkDir)
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

					if currentState == broker.StateError {
						retryKey := chunkID + "_" + compName
						if retryCounts[retryKey] < 3 {
							retryCounts[retryKey]++
							d.MemoryManager.LogToUI(fmt.Sprintf("🔄 [Auto-Recovery] Re-queuing failed chunk %s for %s (Attempt %d/3)", chunkID, compName, retryCounts[retryKey]))
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
						if isBypassed(mediaType, outputFormat, dep) {
							continue
						}

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
						inputPath := filepath.Join(chunkDir, chunkID)
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

		if hasFatalError {
			job.SetStatus(broker.JobPaused)
			d.MemoryManager.LogToUI(fmt.Sprintf("⚠️ [DAG Engine] Job %s suspended after exceeding retry limits. Manual resume required.", jobID))
			return
		}

		if d.isJobComplete(job, mediaType, outputFormat) {
			job.SetStatus(broker.JobDone)
			atomic.AddInt32(&d.MemoryManager.CompletedTasks, 1)
			d.MemoryManager.LogToUI(fmt.Sprintf("🏁 [DAG Engine] Job %s COMPLETED! All branches synchronized.", jobID))
			return
		}
	}
}

func (d *DAGExecutor) isJobComplete(job *broker.JobManifest, mediaType string, outputFormat string) bool {
	job.Mu.Lock()
	defer job.Mu.Unlock()

	for compName, meta := range d.MemoryManager.PipelineDAG {
		if isBypassed(mediaType, outputFormat, compName) {
			continue
		}

		if meta.ExecutionMode == "sequential" && job.GlobalTasks[compName] != broker.StateDone {
			return false
		}

		if meta.ExecutionMode == "chunked" {
			chunkDir := d.getChunkSourceDir(job.JobID, compName)
			entries, err := os.ReadDir(chunkDir)
			if err != nil {
				return false
			}

			bucketCount := 0
			for _, entry := range entries {
				if entry.IsDir() && strings.HasPrefix(entry.Name(), "bucket_") {
					bucketCount++
					chunkID := entry.Name()
					if chunk, exists := job.Chunks[chunkID]; !exists || chunk.Components[compName] != broker.StateDone {
						return false
					}
				}
			}

			if bucketCount == 0 {
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

	pythonExec := d.MemoryManager.GetPythonExec(meta.EnvName)

	cmd := exec.Command(pythonExec, scriptPath, inputFile, outputDir)
	cmd.Env = append(os.Environ(), "PYTHONIOENCODING=utf-8")
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}

	var outBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &outBuf

	if err := cmd.Start(); err != nil {
		d.MemoryManager.LogToUI(fmt.Sprintf("❌ %s failed to start! Error: %v", compName, err))
		job.UpdateGlobalTask(compName, broker.StateError)
		return
	}

	pid := cmd.Process.Pid
	estRAM := d.MemoryManager.TrackCPUStart(compName, pid)

	err := cmd.Wait()
	d.MemoryManager.TrackCPUEnd(compName, pid, estRAM)

	pythonLogs := strings.TrimSpace(outBuf.String())

	if err != nil {
		d.MemoryManager.LogToUI(fmt.Sprintf("❌ %s crashed! Error: %v | Logs: %s", compName, err, pythonLogs))
		job.UpdateGlobalTask(compName, broker.StateError)
		return
	}

	job.UpdateGlobalTask(compName, broker.StateDone)
	d.MemoryManager.LogToUI(fmt.Sprintf("✅ %s finished. Output: %s", compName, pythonLogs))
}
