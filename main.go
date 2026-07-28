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

	pythonExec := filepath.Join(projectRoot, ".venv", "Scripts", "python.exe")

	if _, err := os.Stat(pythonExec); os.IsNotExist(err) {
		panic(fmt.Sprintf("Fatal: Virtual environment not found at %s", pythonExec))
	}

	checkpointManager := broker.NewCheckpointManager()
	memoryManager := broker.NewMemoryManager(5000.0, projectRoot, pythonExec, checkpointManager)
	dagExecutor := pipeline.NewDAGExecutor(memoryManager, checkpointManager, pythonExec, projectRoot)

	for jobID, job := range checkpointManager.ActiveJobs {
		if job.Status == broker.JobPaused {
			job.SetStatus(broker.JobRunning)
			memoryManager.LogToUI(fmt.Sprintf("▶️ Resumed Job %s from Checkpoint", jobID))
		}
		if job.Status != broker.JobDone {
			go dagExecutor.EvaluateJob(jobID)
		}
	}

	app := NewApp(memoryManager, dagExecutor)

	err := wails.Run(&options.App{
		Title:  "EcoFlow DAG Governor",
		Width:  1400,
		Height: 900,
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
		log.Fatal("Error starting EcoFlow GUI:", err.Error())
	}
}
