package main

import (
	"embed"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"ecoflow/internal/broker"
	"ecoflow/internal/pipeline"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
)

//go:embed all:frontend/src
var assets embed.FS

func isProjectRoot(dir string) bool {
	if _, err := os.Stat(filepath.Join(dir, "workspace")); err == nil {
		if _, err := os.Stat(filepath.Join(dir, "pipeline")); err == nil {
			return true
		}
	}
	return false
}

func resolveProjectRoot() string {
	// Priority 1: Check executable path and traverse up
	if exePath, err := os.Executable(); err == nil {
		curr := filepath.Dir(exePath)
		for i := 0; i < 5; i++ {
			if isProjectRoot(curr) {
				return curr
			}
			parent := filepath.Dir(curr)
			if parent == curr {
				break
			}
			curr = parent
		}
	}

	// Priority 2: Check CWD and traverse up
	if cwd, err := os.Getwd(); err == nil {
		curr := cwd
		for i := 0; i < 5; i++ {
			if isProjectRoot(curr) {
				return curr
			}
			parent := filepath.Dir(curr)
			if parent == curr {
				break
			}
			curr = parent
		}
	}

	cwd, _ := os.Getwd()
	return cwd
}

func main() {
	projectRoot := resolveProjectRoot()
	log.Printf("🚀 EcoFlow Governor started. Project Root: %s", projectRoot)

	checkpointManager := broker.NewCheckpointManager()

	// 🛡️ Load existing checkpoints from workspace/jobs
	jobsDir := filepath.Join(projectRoot, "workspace", "jobs")
	if entries, err := os.ReadDir(jobsDir); err == nil {
		for _, entry := range entries {
			if entry.IsDir() && strings.HasPrefix(entry.Name(), "JOB-") {
				checkpointManager.LoadJob(filepath.Join(jobsDir, entry.Name()))
			}
		}
	}

	// 🛡️ NEW: Initialize the Git Sync Manager
	syncManager := broker.NewGitSyncManager(projectRoot)
	//go syncManager.Start()

	// 🛡️ NEW: Pass syncManager to NewMemoryManager
	memoryManager := broker.NewMemoryManager(5000.0, projectRoot, checkpointManager, syncManager)
	dagExecutor := pipeline.NewDAGExecutor(memoryManager, checkpointManager, projectRoot)

	for jobID, job := range checkpointManager.ActiveJobs {
		if job.Status == broker.JobPaused {
			job.ResetIncompleteTasks()
			job.Save()
			memoryManager.LogToUI(fmt.Sprintf("▶️ Resumed Job %s from Checkpoint", jobID))
		}
		if job.Status != broker.JobDone && job.Status != broker.JobAuditASR && job.Status != broker.JobAuditNMT {
			job.ResetIncompleteTasks()
			job.Save()
			go dagExecutor.EvaluateJob(jobID)
		}
	}

	app := NewApp(memoryManager, dagExecutor)

	err := wails.Run(&options.App{
		Title:            "Echoflow DAG Governor",
		Width:            1400,
		Height:           900,
		WindowStartState: options.Maximised,
		AssetServer: &assetserver.Options{
			Assets: assets,
		},
		BackgroundColour: &options.RGBA{R: 2, G: 6, B: 23, A: 1},
		OnStartup:        app.startup,
		Bind: []interface{}{
			app,
		},
	})

	if err != nil {
		log.Fatal("Error starting Echoflow GUI:", err.Error())
	}
}
