package broker

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/wailsapp/wails/v2/pkg/runtime"
)

type BucketTask struct {
	JobID     string
	ChunkID   string
	Component string
	InputData string
}

type ModelConfig struct {
	ModelPath      string  `json:"model_path"`
	Framework      string  `json:"framework"`
	EstimatedRamMB float64 `json:"estimated_ram_mb"`
}

type PipelineComponent struct {
	Domain         string   `json:"domain"`
	ExecutionMode  string   `json:"execution_mode"`
	DependsOn      []string `json:"depends_on"`
	ModelRef       string   `json:"model_ref,omitempty"`
	Script         string   `json:"script"`
	AcceptedInputs []string `json:"accepted_inputs"`
	Produces       string   `json:"produces"`
}

type WarmWorker struct {
	PID          int     `json:"pid"`
	Component    string  `json:"component"`
	ActualRamMB  float64 `json:"actual_ram_mb"`
	Status       string  `json:"status"`
	IsActive     bool    `json:"is_active"`
	CurrentChunk string  `json:"current_chunk"`
	ProgressPct  int     `json:"progress_pct"`
	Stdin        io.WriteCloser
	Stdout       *bufio.Scanner
	Cmd          *exec.Cmd
}

type TelemetrySnapshot struct {
	MaxRAMMB       float64                   `json:"max_ram"`
	CurrentRAMMB   float64                   `json:"current_ram"`
	ActiveDaemons  int                       `json:"active_daemons"`
	IdleDaemons    int                       `json:"idle_daemons"`
	TotalCompleted int32                     `json:"completed"`
	QueueBacklogs  map[string]int            `json:"queue_backlogs"`
	WorkerStats    []WorkerStat              `json:"worker_stats"`
	RecentAlerts   []string                  `json:"alerts"`
	ChunkProgress  map[string]map[string]int `json:"chunk_progress"`
	GlobalTasks    map[string]string         `json:"global_tasks"`
	JobComplete    bool                      `json:"job_complete"`
}

type WorkerStat struct {
	PID          int     `json:"pid"`
	Component    string  `json:"component"`
	RAM          float64 `json:"ram_mb"`
	Status       string  `json:"status"`
	CurrentChunk string  `json:"current_chunk"`
	ProgressPct  int     `json:"progress_pct"`
}

type MemoryManager struct {
	PendingBuckets map[string][]BucketTask
	ModelRegistry  map[string]ModelConfig
	PipelineDAG    map[string]PipelineComponent
	MaxRAMMB       float64
	CurrentRAMMB   float64
	ActiveWorkers  []*WarmWorker
	BootingWorkers map[string]int
	mu             sync.Mutex
	PythonExec     string
	ProjectRoot    string
	Checkpoints    *CheckpointManager
	RecentAlerts   []string
	CompletedTasks int32
	ctx            context.Context
}

func NewMemoryManager(maxRamMB float64, projectRoot, pythonExec string, cm *CheckpointManager) *MemoryManager {
	mgr := &MemoryManager{
		PendingBuckets: make(map[string][]BucketTask),
		ModelRegistry:  make(map[string]ModelConfig),
		PipelineDAG:    make(map[string]PipelineComponent),
		MaxRAMMB:       maxRamMB,
		PythonExec:     pythonExec,
		ProjectRoot:    projectRoot,
		Checkpoints:    cm,
		BootingWorkers: make(map[string]int),
		RecentAlerts:   make([]string, 0),
	}
	mgr.loadConfigs()
	go mgr.startAutoScalerLoop()
	return mgr
}

func (m *MemoryManager) loadConfigs() {
	regPath := filepath.Join(m.ProjectRoot, "models", "registry.json")
	if file, err := os.ReadFile(regPath); err == nil {
		json.Unmarshal(file, &m.ModelRegistry)
	}
	dagPath := filepath.Join(m.ProjectRoot, "pipeline", "manifest.json")
	if file, err := os.ReadFile(dagPath); err == nil {
		json.Unmarshal(file, &m.PipelineDAG)
	}
}

func (m *MemoryManager) LogToUI(msg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RecentAlerts = append([]string{time.Now().Format("15:04:05") + " " + msg}, m.RecentAlerts...)
	if len(m.RecentAlerts) > 25 {
		m.RecentAlerts = m.RecentAlerts[:25]
	}
}

func (m *MemoryManager) PushBucket(task BucketTask) {
	job := m.Checkpoints.GetJob(task.JobID)
	if job != nil && job.Status == JobPaused {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.PendingBuckets[task.Component] = append(m.PendingBuckets[task.Component], task)
}

func (m *MemoryManager) EvaluateQueuesNow() {
	go m.evaluateQueues()
}

func (m *MemoryManager) startAutoScalerLoop() {
	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		m.evaluateQueues()
	}
}

func (m *MemoryManager) evaluateQueues() {
	m.PruneExcessWorkers()
	m.recoverStaleQueued()

	m.mu.Lock()
	defer m.mu.Unlock()

	var activeComps []string
	for comp, queue := range m.PendingBuckets {
		if len(queue) > 0 {
			activeComps = append(activeComps, comp)
		}
	}
	if len(activeComps) == 0 {
		return
	}

	sort.Slice(activeComps, func(i, j int) bool {
		metaI := m.PipelineDAG[activeComps[i]]
		metaJ := m.PipelineDAG[activeComps[j]]
		ramI := m.ModelRegistry[metaI.ModelRef].EstimatedRamMB
		ramJ := m.ModelRegistry[metaJ.ModelRef].EstimatedRamMB
		return ramI > ramJ
	})

	for _, comp := range activeComps {
		queue := m.PendingBuckets[comp]
		if len(queue) == 0 {
			continue
		}

		meta := m.PipelineDAG[comp]
		estRAM := m.ModelRegistry[meta.ModelRef].EstimatedRamMB

		workerCount := 0
		var idleWorker *WarmWorker
		for _, w := range m.ActiveWorkers {
			if w.Component == comp {
				workerCount++
				if !w.IsActive {
					idleWorker = w
				}
			}
		}

		totalThreads := workerCount + m.BootingWorkers[comp]

		if idleWorker == nil {
			if totalThreads < 1 {
				m.BootingWorkers[comp]++
				if m.CurrentRAMMB+estRAM <= m.MaxRAMMB {
					m.mu.Unlock()
					newWorker := m.spawnWorkerDynamic(comp, meta)
					m.mu.Lock()

					m.BootingWorkers[comp]--
					if newWorker == nil {
						continue
					}
					idleWorker = newWorker
				} else {
					m.BootingWorkers[comp]--
					continue
				}
			} else {
				continue
			}
		}

		if len(m.PendingBuckets[comp]) > 0 {
			task := m.PendingBuckets[comp][0]
			m.PendingBuckets[comp] = m.PendingBuckets[comp][1:]
			idleWorker.IsActive = true
			idleWorker.CurrentChunk = task.ChunkID
			idleWorker.ProgressPct = 0
			go m.executeTask(idleWorker, task)
		}
	}
}

func (m *MemoryManager) spawnWorkerDynamic(comp string, meta PipelineComponent) *WarmWorker {
	estRAM := m.ModelRegistry[meta.ModelRef].EstimatedRamMB
	m.CurrentRAMMB += estRAM

	cmd := exec.Command(m.PythonExec, filepath.Join(m.ProjectRoot, meta.Script))
	cmd.Env = append(os.Environ(), "PYTHONIOENCODING=utf-8")
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}

	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	stderr, _ := cmd.StderrPipe()

	cmd.Start()

	scanner := bufio.NewScanner(stdout)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	errScanner := bufio.NewScanner(stderr)

	handshakeFound := false
	var actualRamMB float64

	for i := 0; i < 100; i++ {
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()

		idx := strings.Index(line, "ECOFLOW_IPC__")
		if idx != -1 {
			jsonStr := line[idx+len("ECOFLOW_IPC__"):]
			var resp struct {
				Status      string  `json:"status"`
				ActualRamMB float64 `json:"actual_ram_mb"`
			}
			if err := json.Unmarshal([]byte(jsonStr), &resp); err == nil && resp.Status == "ready" {
				handshakeFound = true
				actualRamMB = resp.ActualRamMB
				break
			}
		} else {
			if strings.TrimSpace(line) != "" {
				m.LogToUI(fmt.Sprintf("ℹ️ [%s Boot] %s", comp, line))
			}
		}
	}

	if handshakeFound {
		correction := actualRamMB - estRAM
		m.CurrentRAMMB += correction
		m.LogToUI(fmt.Sprintf("🚀 [%s] Booted via %s. OS RAM: %.1f MB", comp, meta.ModelRef, actualRamMB))

		worker := &WarmWorker{
			PID:         cmd.Process.Pid,
			Component:   comp,
			ActualRamMB: actualRamMB,
			Stdin:       stdin,
			Stdout:      scanner,
			Cmd:         cmd,
			IsActive:    false,
		}
		m.ActiveWorkers = append(m.ActiveWorkers, worker)

		go func() {
			for errScanner.Scan() {
				txt := strings.ToLower(errScanner.Text())
				if !strings.Contains(txt, "xnnpack") &&
					!strings.Contains(txt, "inference_feedback_manager") &&
					!strings.Contains(txt, "created tensorflow lite") &&
					!strings.Contains(txt, "clearcut") &&
					!strings.Contains(txt, "source location trace") &&
					!strings.Contains(txt, "failed_precondition") {
					m.LogToUI(fmt.Sprintf("⚠️ [%s] STDERR: %s", comp, errScanner.Text()))
				}
			}
		}()
		return worker
	}

	m.LogToUI(fmt.Sprintf("❌ [%s] Daemon failed to send valid handshake.", comp))
	m.CurrentRAMMB -= estRAM
	time.Sleep(2 * time.Second)
	return nil
}

func (m *MemoryManager) executeTask(worker *WarmWorker, task BucketTask) {
	defer func() {
		m.mu.Lock()
		worker.IsActive = false
		worker.CurrentChunk = ""
		worker.ProgressPct = 0
		m.mu.Unlock()

		go func() {
			m.evaluateQueues()
			m.PruneExcessWorkers()
		}()
	}()

	job := m.Checkpoints.GetJob(task.JobID)
	if job == nil {
		return
	}

	job.UpdateChunkState(task.ChunkID, task.Component, StateRunning)
	req := map[string]string{"input": task.InputData}
	reqBytes, _ := json.Marshal(req)

	m.LogToUI(fmt.Sprintf("⚡ [%s] Processing chunk %s...", task.Component, task.ChunkID))
	worker.Stdin.Write(reqBytes)
	worker.Stdin.Write([]byte("\n"))

	for worker.Stdout.Scan() {
		respLine := worker.Stdout.Text()

		idx := strings.Index(respLine, "ECOFLOW_IPC__")
		if idx != -1 {
			jsonStr := respLine[idx+len("ECOFLOW_IPC__"):]
			var resp map[string]interface{}

			if err := json.Unmarshal([]byte(jsonStr), &resp); err == nil {
				if resp["status"] == "success" {
					job.UpdateChunkState(task.ChunkID, task.Component, StateDone)
					m.LogToUI(fmt.Sprintf("✅ [%s] Chunk %s completed!", task.Component, task.ChunkID))
					return
				} else if resp["status"] == "progress" {
					m.mu.Lock()
					if chunkVal, ok := resp["chunk"].(string); ok {
						worker.CurrentChunk = chunkVal
					}
					if pctVal, ok := resp["pct"].(float64); ok {
						worker.ProgressPct = int(pctVal)
					}
					m.mu.Unlock()
				} else {
					job.UpdateChunkState(task.ChunkID, task.Component, StateError)
					m.LogToUI(fmt.Sprintf("❌ [%s] Chunk Error: %s", task.Component, respLine))
					return
				}
			} else {
				m.LogToUI(fmt.Sprintf("❌ [%s] IPC Parse Error: %v", task.Component, err))
			}
		} else {
			if strings.TrimSpace(respLine) != "" {
				m.LogToUI(fmt.Sprintf("ℹ️ [%s] %s", task.Component, respLine))
			}
			continue
		}
	}

	job.UpdateChunkState(task.ChunkID, task.Component, StateError)
	m.LogToUI(fmt.Sprintf("❌ [%s] Daemon died unexpectedly on %s", task.Component, task.ChunkID))

	m.mu.Lock()
	m.CurrentRAMMB -= worker.ActualRamMB
	for i, w := range m.ActiveWorkers {
		if w == worker {
			m.ActiveWorkers = append(m.ActiveWorkers[:i], m.ActiveWorkers[i+1:]...)
			break
		}
	}
	m.mu.Unlock()
}

func (m *MemoryManager) recoverStaleQueued() {
	m.Checkpoints.mu.RLock()
	jobs := make([]*JobManifest, 0, len(m.Checkpoints.ActiveJobs))
	jobIDs := make([]string, 0, len(m.Checkpoints.ActiveJobs))
	for id, job := range m.Checkpoints.ActiveJobs {
		if job.Status == JobRunning {
			jobs = append(jobs, job)
			jobIDs = append(jobIDs, id)
		}
	}
	m.Checkpoints.mu.RUnlock()

	for i, job := range jobs {
		staleChunks := job.GetStaleQueuedChunks()
		if len(staleChunks) == 0 {
			continue
		}

		m.mu.Lock()
		for _, sc := range staleChunks {
			alreadyQueued := false
			for _, pending := range m.PendingBuckets[sc.Component] {
				if pending.ChunkID == sc.ChunkID && pending.JobID == jobIDs[i] {
					alreadyQueued = true
					break
				}
			}

			activelyProcessing := false
			for _, w := range m.ActiveWorkers {
				if w.IsActive && w.CurrentChunk == sc.ChunkID && w.Component == sc.Component {
					activelyProcessing = true
					break
				}
			}

			if !alreadyQueued && !activelyProcessing {
				job.UpdateChunkState(sc.ChunkID, sc.Component, StatePending)
				m.mu.Unlock()
				m.LogToUI(fmt.Sprintf("🔄 [Recovery] Re-queued stale %s/%s", sc.Component, sc.ChunkID))
				m.mu.Lock()
			}
		}
		m.mu.Unlock()
	}
}

func (m *MemoryManager) PruneExcessWorkers() {
	m.mu.Lock()

	totalPending := 0
	for _, q := range m.PendingBuckets {
		totalPending += len(q)
	}

	isAnyWorkerActive := false
	for _, w := range m.ActiveWorkers {
		if w.IsActive {
			isAnyWorkerActive = true
			break
		}
	}

	killAllIdle := (totalPending == 0) && !isAnyWorkerActive

	idleCounts := make(map[string]int)
	var retainedWorkers []*WarmWorker

	// 🛡️ THE FIX: Accumulate logs safely instead of calling LogToUI while memory is locked
	var logsToEmit []string

	for _, w := range m.ActiveWorkers {
		if w.IsActive {
			retainedWorkers = append(retainedWorkers, w)
			continue
		}

		if killAllIdle {
			if w.Cmd != nil && w.Cmd.Process != nil {
				w.Cmd.Process.Kill()
			}
			m.CurrentRAMMB -= w.ActualRamMB
			logsToEmit = append(logsToEmit, fmt.Sprintf("🛑 [Auto-Scaler] Terminated %s daemon (Pipeline Idle).", w.Component))
			continue
		}

		idleCounts[w.Component]++
		if idleCounts[w.Component] > 1 {
			if w.Cmd != nil && w.Cmd.Process != nil {
				w.Cmd.Process.Kill()
			}
			m.CurrentRAMMB -= w.ActualRamMB
			logsToEmit = append(logsToEmit, fmt.Sprintf("🧹 [Auto-Scaler] Terminated excess %s thread.", w.Component))
		} else {
			retainedWorkers = append(retainedWorkers, w)
		}
	}
	m.ActiveWorkers = retainedWorkers

	// 🛡️ THE FIX: Unlock memory BEFORE sending the logs to the UI
	m.mu.Unlock()

	for _, msg := range logsToEmit {
		m.LogToUI(msg)
	}
}

func (m *MemoryManager) StartTelemetryEmitter(ctx context.Context) {
	m.ctx = ctx
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.mu.Lock()
			snap := TelemetrySnapshot{
				MaxRAMMB:       m.MaxRAMMB,
				CurrentRAMMB:   m.CurrentRAMMB,
				TotalCompleted: atomic.LoadInt32(&m.CompletedTasks),
				QueueBacklogs:  make(map[string]int),
				RecentAlerts:   make([]string, len(m.RecentAlerts)),
			}
			copy(snap.RecentAlerts, m.RecentAlerts)
			for comp, q := range m.PendingBuckets {
				snap.QueueBacklogs[comp] = len(q)
			}

			snap.ChunkProgress = make(map[string]map[string]int)
			for compName := range m.PipelineDAG {
				snap.ChunkProgress[compName] = make(map[string]int)
			}

			for _, w := range m.ActiveWorkers {
				status := "IDLE"
				if w.IsActive {
					status = "ACTIVE"
					snap.ActiveDaemons++
					if w.CurrentChunk != "" {
						snap.ChunkProgress[w.Component][w.CurrentChunk] = w.ProgressPct
					}
				} else {
					snap.IdleDaemons++
				}
				snap.WorkerStats = append(snap.WorkerStats, WorkerStat{
					PID:          w.PID,
					Component:    w.Component,
					RAM:          w.ActualRamMB,
					Status:       status,
					CurrentChunk: w.CurrentChunk,
					ProgressPct:  w.ProgressPct,
				})
			}
			m.mu.Unlock()

			snap.GlobalTasks = make(map[string]string)
			if m.Checkpoints != nil {
				m.Checkpoints.mu.RLock()
				var latestJob string
				for id := range m.Checkpoints.ActiveJobs {
					if id > latestJob {
						latestJob = id
					}
				}
				if latestJob != "" {
					job := m.Checkpoints.ActiveJobs[latestJob]
					job.Mu.Lock()
					for k, v := range job.GlobalTasks {
						snap.GlobalTasks[k] = string(v)
					}
					if job.Status == "COMPLETED" {
						snap.JobComplete = true
					}
					job.Mu.Unlock()
				}
				m.Checkpoints.mu.RUnlock()
			}

			runtime.EventsEmit(m.ctx, "telemetry_update", snap)
		}
	}
}

func (m *MemoryManager) InjectJob(targetPath string) error {
	targetPath = strings.Trim(strings.TrimSpace(targetPath), "\"'")
	m.LogToUI(fmt.Sprintf("📥 Initiating Injection: %s", targetPath))

	src, err := os.Open(targetPath)
	if err != nil {
		m.LogToUI(fmt.Sprintf("⚠️ Failed to open target file: %v", err))
		return err
	}
	defer src.Close()

	fileName := filepath.Base(targetPath)
	inboxDir := filepath.Join(m.ProjectRoot, "workspace", "inbox")
	os.MkdirAll(inboxDir, 0755)

	tmpPath := filepath.Join(inboxDir, fileName+".tmp")
	finalPath := filepath.Join(inboxDir, fileName)

	dst, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	io.Copy(dst, src)
	dst.Close()

	os.Rename(tmpPath, finalPath)
	m.LogToUI(fmt.Sprintf("✅ Dropped %s into Inbox!", fileName))
	return nil
}
