package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

// SubmitJob securely locks the asset in the Job directory before firing the AI
func (a *App) SubmitJob(targetPath string) error {
	targetPath = strings.Trim(strings.TrimSpace(targetPath), "\"'")
	a.MM.LogToUI(fmt.Sprintf("📥 Initiating Injection: %s", targetPath))

	if _, err := os.Stat(targetPath); os.IsNotExist(err) {
		errMsg := fmt.Sprintf("⚠️ File does not exist: %s", targetPath)
		a.MM.LogToUI(errMsg)
		return fmt.Errorf(errMsg)
	}

	jobID := fmt.Sprintf("JOB-%d", time.Now().Unix())
	jobDir := filepath.Join(a.MM.ProjectRoot, "workspace", "jobs", jobID)
	os.MkdirAll(jobDir, 0755)

	fileName := filepath.Base(targetPath)
	destPath := filepath.Join(jobDir, fileName)

	src, err := os.Open(targetPath)
	if err != nil {
		a.MM.LogToUI(fmt.Sprintf("⚠️ Failed to open target file: %v", err))
		return err
	}
	defer src.Close()

	dst, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	if err != nil {
		a.MM.LogToUI(fmt.Sprintf("⚠️ Failed to securely copy file: %v", err))
		return err
	}

	a.MM.Checkpoints.InitializeJob(jobID, destPath, filepath.Join(a.MM.ProjectRoot, "workspace"))
	a.MM.LogToUI(fmt.Sprintf("✅ Job %s safely created and queued!", jobID))

	go a.DAG.EvaluateJob(jobID)
	return nil
}
