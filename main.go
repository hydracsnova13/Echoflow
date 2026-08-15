package main

import (
	"embed"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"ecoflow/internal/broker"
	"ecoflow/internal/pipeline"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
)

//go:embed all:frontend/src
var assets embed.FS

func main() {
	cwd, _ := os.Getwd()
	projectRoot := cwd

	if filepath.Base(cwd) == "bin" && filepath.Base(filepath.Dir(cwd)) == "build" {
		projectRoot = filepath.Dir(filepath.Dir(cwd))
	}

	checkpointManager := broker.NewCheckpointManager()

	// 🛡️ Removed global pythonExec, now injected dynamically by environment
	memoryManager := broker.NewMemoryManager(5000.0, projectRoot, checkpointManager)
	dagExecutor := pipeline.NewDAGExecutor(memoryManager, checkpointManager, projectRoot)

	for jobID, job := range checkpointManager.ActiveJobs {
		if job.Status == broker.JobPaused {
			job.ResetIncompleteTasks()
			job.Save()
			memoryManager.LogToUI(fmt.Sprintf("▶️ Resumed Job %s from Checkpoint", jobID))
		}
		if job.Status != broker.JobDone {
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
