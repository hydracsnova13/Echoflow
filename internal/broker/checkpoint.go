package broker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
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
	JobRunning  JobStatus = "RUNNING"
	JobPaused   JobStatus = "PAUSED"
	JobDone     JobStatus = "COMPLETED"
	JobAuditASR JobStatus = "AUDIT_ASR"
	JobAuditNMT JobStatus = "AUDIT_NMT"
)

type ChunkState struct {
	Components map[string]ExecutionState `json:"components"`
}

type JobManifest struct {
	JobID        string                    `json:"job_id"`
	Status       JobStatus                 `json:"status"`
	SourceFile   string                    `json:"source_file"`
	AuditASRDone bool                      `json:"audit_asr_done"`
	AuditNMTDone bool                      `json:"audit_nmt_done"`
	GlobalTasks  map[string]ExecutionState `json:"global_tasks"`
	Chunks       map[string]*ChunkState    `json:"chunks"`
	Mu           sync.Mutex                `json:"-"`
	manifestDir  string                    `json:"-"`
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
		JobID:        jobID,
		Status:       JobRunning,
		SourceFile:   sourceFile,
		AuditASRDone: false,
		AuditNMTDone: false,
		GlobalTasks:  make(map[string]ExecutionState),
		Chunks:       make(map[string]*ChunkState),
		manifestDir:  jobDir,
	}

	if data, err := os.ReadFile(manifestPath); err == nil {
		json.Unmarshal(data, manifest)
	}
	cm.ActiveJobs[jobID] = manifest
	manifest.Save()
	return manifest
}

func (cm *CheckpointManager) LoadJob(jobDir string) *JobManifest {
	manifestPath := filepath.Join(jobDir, "manifest.json")
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil
	}

	manifest := &JobManifest{
		GlobalTasks: make(map[string]ExecutionState),
		Chunks:      make(map[string]*ChunkState),
		manifestDir: jobDir,
	}

	if err := json.Unmarshal(data, manifest); err != nil {
		return nil
	}

	cm.mu.Lock()
	cm.ActiveJobs[manifest.JobID] = manifest
	cm.mu.Unlock()

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

	// 🛡️ THE FIX: Retry mechanism to bypass Windows file locks caused by rapid UI polling
	for i := 0; i < 10; i++ {
		if err := os.Rename(tmpPath, path); err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
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
	if jm.Status != JobAuditASR && jm.Status != JobAuditNMT {
		jm.Status = JobRunning
	}
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
