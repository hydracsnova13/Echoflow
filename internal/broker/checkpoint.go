package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

type ExecutionState string

const (
	StatePending ExecutionState = "PENDING"
	StateQueued  ExecutionState = "QUEUED"
	StateRunning ExecutionState = "RUNNING"
	StateDone    ExecutionState = "DONE"
	StateError   ExecutionState = "ERROR"
)

type JobStatus string

const (
	JobRunning JobStatus = "RUNNING"
	JobPaused  JobStatus = "PAUSED"
	JobDone    JobStatus = "COMPLETED"
)

type ChunkState struct {
	Components map[string]ExecutionState `json:"components"`
}

type JobManifest struct {
	JobID       string                    `json:"job_id"`
	Status      JobStatus                 `json:"status"`
	SourceFile  string                    `json:"source_file"`
	GlobalTasks map[string]ExecutionState `json:"global_tasks"`
	Chunks      map[string]*ChunkState    `json:"chunks"`
	Mu          sync.Mutex                `json:"-"`
	manifestDir string                    `json:"-"`
}

type CheckpointManager struct {
	ActiveJobs map[string]*JobManifest
	mu         sync.RWMutex
}

func NewCheckpointManager() *CheckpointManager {
	return &CheckpointManager{
		ActiveJobs: make(map[string]*JobManifest),
	}
}

func (cm *CheckpointManager) InitializeJob(jobID, sourceFile, workspaceRoot string) *JobManifest {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	jobDir := filepath.Join(workspaceRoot, "jobs", jobID)
	os.MkdirAll(jobDir, 0755)
	manifestPath := filepath.Join(jobDir, "manifest.json")

	manifest := &JobManifest{
		JobID:       jobID,
		Status:      JobRunning,
		SourceFile:  sourceFile,
		GlobalTasks: make(map[string]ExecutionState),
		Chunks:      make(map[string]*ChunkState),
		manifestDir: jobDir,
	}

	if data, err := os.ReadFile(manifestPath); err == nil {
		json.Unmarshal(data, manifest)
	}
	cm.ActiveJobs[jobID] = manifest
	manifest.Save()
	return manifest
}

func (cm *CheckpointManager) GetJob(jobID string) *JobManifest {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return cm.ActiveJobs[jobID]
}

func (jm *JobManifest) Save() {
	jm.Mu.Lock()
	defer jm.Mu.Unlock()
	data, _ := json.MarshalIndent(jm, "", "  ")
	path := filepath.Join(jm.manifestDir, "manifest.json")
	tmpPath := path + ".tmp"
	os.WriteFile(tmpPath, data, 0644)
	os.Rename(tmpPath, path)
}

func (jm *JobManifest) UpdateGlobalTask(component string, state ExecutionState) {
	jm.Mu.Lock()
	jm.GlobalTasks[component] = state
	jm.Mu.Unlock()
	jm.Save()
}

func (jm *JobManifest) UpdateChunkState(chunkID, component string, state ExecutionState) {
	jm.Mu.Lock()
	if _, exists := jm.Chunks[chunkID]; !exists {
		jm.Chunks[chunkID] = &ChunkState{Components: make(map[string]ExecutionState)}
	}
	jm.Chunks[chunkID].Components[component] = state
	jm.Mu.Unlock()
	jm.Save()
}

func (jm *JobManifest) SetStatus(status JobStatus) {
	jm.Mu.Lock()
	jm.Status = status
	jm.Mu.Unlock()
	jm.Save()
}

// 🛡️ ResetIncompleteTasks resets all non-completed tasks (RUNNING, QUEUED, ERROR) back to PENDING/unstarted
// so that resuming a paused or crashed job will cleanly re-execute them.
func (jm *JobManifest) ResetIncompleteTasks() {
	jm.Mu.Lock()
	defer jm.Mu.Unlock()

	for k, v := range jm.GlobalTasks {
		if v != StateDone {
			delete(jm.GlobalTasks, k)
		}
	}
	for _, chunk := range jm.Chunks {
		for comp, state := range chunk.Components {
			if state != StateDone {
				chunk.Components[comp] = StatePending
			}
		}
	}
	jm.Status = JobRunning
}

type StaleChunk struct {
	ChunkID   string
	Component string
}

func (jm *JobManifest) GetStaleQueuedChunks() []StaleChunk {
	jm.Mu.Lock()
	defer jm.Mu.Unlock()

	var stale []StaleChunk
	for chunkID, chunk := range jm.Chunks {
		for comp, state := range chunk.Components {
			if state == StateQueued || state == StateRunning {
				stale = append(stale, StaleChunk{ChunkID: chunkID, Component: comp})
			}
		}
	}
	return stale
}
