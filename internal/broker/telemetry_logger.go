package broker

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// MaxLogStorageBytes is the 1 GB threshold for log storage
	MaxLogStorageBytes = int64(1024 * 1024 * 1024)
)

type LogStorageStats struct {
	TotalSizeBytes          int64    `json:"total_size_bytes"`
	TotalSizeFormatted      string   `json:"total_size_formatted"`
	MaxSizeBytes            int64    `json:"max_size_bytes"`
	MaxSizeFormatted        string   `json:"max_size_formatted"`
	UsagePercent            float64  `json:"usage_percent"`
	IsNearThreshold         bool     `json:"is_near_threshold"`
	GlobalLogCount          int      `json:"global_log_count"`
	JobLogCount             int      `json:"job_log_count"`
	OldestLogDate           string   `json:"oldest_log_date"`
	NewestLogDate           string   `json:"newest_log_date"`
	RecentEntries           []string `json:"recent_entries"`
}

type DeleteResult struct {
	DeletedFilesCount       int    `json:"deleted_files_count"`
	FreedBytes              int64  `json:"freed_bytes"`
	FreedBytesFormatted     string `json:"freed_bytes_formatted"`
	RemainingBytes          int64  `json:"remaining_bytes"`
	RemainingBytesFormatted string `json:"remaining_bytes_formatted"`
	Message                 string `json:"message"`
}

type TelemetryLogger struct {
	ProjectRoot   string
	GlobalLogsDir string
	JobsDir       string
	mu            sync.RWMutex
}

func NewTelemetryLogger(projectRoot string) *TelemetryLogger {
	globalDir := filepath.Join(projectRoot, "workspace", "logs")
	jobsDir := filepath.Join(projectRoot, "workspace", "jobs")
	os.MkdirAll(globalDir, 0755)

	return &TelemetryLogger{
		ProjectRoot:   projectRoot,
		GlobalLogsDir: globalDir,
		JobsDir:       jobsDir,
	}
}

// Log writes a timestamped record to the central daily Stackdriver log,
// and if jobID is specified, mirrors the record into the specific job's directory.
func (tl *TelemetryLogger) Log(level, scope, jobID, message string) {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	now := time.Now()
	timestamp := now.Format("2006-01-02 15:04:05.000")
	cleanMsg := strings.TrimSpace(message)
	if cleanMsg == "" {
		return
	}

	var entry string
	if jobID != "" {
		entry = fmt.Sprintf("[%s] [%s] [%s] [%s] %s\n", timestamp, strings.ToUpper(level), strings.ToUpper(scope), jobID, cleanMsg)
	} else {
		entry = fmt.Sprintf("[%s] [%s] [%s] %s\n", timestamp, strings.ToUpper(level), strings.ToUpper(scope), cleanMsg)
	}

	// 1. Central Stackdriver: Append to daily log file
	dateStr := now.Format("2006-01-02")
	globalLogPath := filepath.Join(tl.GlobalLogsDir, fmt.Sprintf("stackdriver_%s.log", dateStr))
	if f, err := os.OpenFile(globalLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
		f.WriteString(entry)
		f.Close()
	}

	// 2. Job-specific Log: If jobID is provided, mirror into workspace/jobs/<jobID>/job_telemetry.log
	if jobID != "" {
		jobDir := filepath.Join(tl.JobsDir, jobID)
		if fi, err := os.Stat(jobDir); err == nil && fi.IsDir() {
			jobLogPath := filepath.Join(jobDir, "job_telemetry.log")
			if jf, err := os.OpenFile(jobLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644); err == nil {
				jf.WriteString(entry)
				jf.Close()
			}
		}
	}
}

func (tl *TelemetryLogger) LogGlobal(level, scope, message string) {
	tl.Log(level, scope, "", message)
}

func (tl *TelemetryLogger) LogJob(jobID, level, scope, message string) {
	tl.Log(level, scope, jobID, message)
}

func FormatByteSize(bytes int64) string {
	const (
		KB = 1024
		MB = 1024 * KB
		GB = 1024 * MB
	)

	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/float64(GB))
	case bytes >= MB:
		return fmt.Sprintf("%.1f MB", float64(bytes)/float64(MB))
	case bytes >= KB:
		return fmt.Sprintf("%.1f KB", float64(bytes)/float64(KB))
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

// GetStorageStats scans the central logs directory and all job directories to calculate current size,
// usage percentage against the 1 GB threshold, file counts, and recent entries.
func (tl *TelemetryLogger) GetStorageStats() (LogStorageStats, error) {
	tl.mu.RLock()
	defer tl.mu.RUnlock()

	var totalBytes int64
	var globalCount int
	var jobCount int
	var logDates []string

	// Scan global logs
	if entries, err := os.ReadDir(tl.GlobalLogsDir); err == nil {
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
				continue
			}
			globalCount++
			if info, err := e.Info(); err == nil {
				totalBytes += info.Size()
				logDates = append(logDates, info.ModTime().Format("2006-01-02"))
			}
		}
	}

	// Scan job-specific logs in workspace/jobs/*/job_telemetry.log
	if jobEntries, err := os.ReadDir(tl.JobsDir); err == nil {
		for _, j := range jobEntries {
			if !j.IsDir() {
				continue
			}
			jobLogPath := filepath.Join(tl.JobsDir, j.Name(), "job_telemetry.log")
			if fi, err := os.Stat(jobLogPath); err == nil && !fi.IsDir() {
				jobCount++
				totalBytes += fi.Size()
				logDates = append(logDates, fi.ModTime().Format("2006-01-02"))
			}
		}
	}

	oldestDate := "N/A"
	newestDate := "N/A"
	if len(logDates) > 0 {
		sort.Strings(logDates)
		oldestDate = logDates[0]
		newestDate = logDates[len(logDates)-1]
	}

	usagePercent := (float64(totalBytes) / float64(MaxLogStorageBytes)) * 100.0
	if usagePercent > 100.0 {
		usagePercent = 100.0
	}

	isNearThreshold := totalBytes >= (MaxLogStorageBytes * 80 / 100) // 80% of 1 GB

	recent, _ := tl.getRecentEntriesInternal(50)

	return LogStorageStats{
		TotalSizeBytes:     totalBytes,
		TotalSizeFormatted: FormatByteSize(totalBytes),
		MaxSizeBytes:       MaxLogStorageBytes,
		MaxSizeFormatted:   "1.00 GB",
		UsagePercent:       usagePercent,
		IsNearThreshold:    isNearThreshold,
		GlobalLogCount:     globalCount,
		JobLogCount:        jobCount,
		OldestLogDate:      oldestDate,
		NewestLogDate:      newestDate,
		RecentEntries:      recent,
	}, nil
}

func (tl *TelemetryLogger) getRecentEntriesInternal(limit int) ([]string, error) {
	var lines []string

	// Read newest global log files first
	entries, err := os.ReadDir(tl.GlobalLogsDir)
	if err != nil {
		return lines, nil
	}

	var logFiles []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".log") {
			logFiles = append(logFiles, filepath.Join(tl.GlobalLogsDir, e.Name()))
		}
	}

	// Sort newest first
	sort.Sort(sort.Reverse(sort.StringSlice(logFiles)))

	for _, file := range logFiles {
		if len(lines) >= limit {
			break
		}
		f, err := os.Open(file)
		if err != nil {
			continue
		}

		var fileLines []string
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			fileLines = append(fileLines, scanner.Text())
		}
		f.Close()

		// Read file lines in reverse
		for i := len(fileLines) - 1; i >= 0; i-- {
			if len(lines) >= limit {
				break
			}
			lines = append(lines, fileLines[i])
		}
	}

	return lines, nil
}

func (tl *TelemetryLogger) GetRecentLogs(limit int) ([]string, error) {
	tl.mu.RLock()
	defer tl.mu.RUnlock()
	return tl.getRecentEntriesInternal(limit)
}

// DeleteLogsOlderThan purges all log files timestamped strictly before cutoff date.
func (tl *TelemetryLogger) DeleteLogsOlderThan(cutoff time.Time) (DeleteResult, error) {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	var deletedCount int
	var freedBytes int64

	cutoffDateStr := cutoff.Format("2006-01-02")

	// 1. Purge global log files older than cutoff
	if entries, err := os.ReadDir(tl.GlobalLogsDir); err == nil {
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
				continue
			}

			shouldDelete := false
			// Check filename date pattern stackdriver_YYYY-MM-DD.log
			if strings.HasPrefix(e.Name(), "stackdriver_") {
				datePart := strings.TrimPrefix(e.Name(), "stackdriver_")
				datePart = strings.TrimSuffix(datePart, ".log")
				if datePart < cutoffDateStr {
					shouldDelete = true
				}
			} else if info, err := e.Info(); err == nil {
				if info.ModTime().Before(cutoff) {
					shouldDelete = true
				}
			}

			if shouldDelete {
				filePath := filepath.Join(tl.GlobalLogsDir, e.Name())
				if fi, err := os.Stat(filePath); err == nil {
					freedBytes += fi.Size()
				}
				if err := os.Remove(filePath); err == nil {
					deletedCount++
				}
			}
		}
	}

	// 2. Purge job-specific logs if job log is older than cutoff
	if jobEntries, err := os.ReadDir(tl.JobsDir); err == nil {
		for _, j := range jobEntries {
			if !j.IsDir() {
				continue
			}
			jobLogPath := filepath.Join(tl.JobsDir, j.Name(), "job_telemetry.log")
			if fi, err := os.Stat(jobLogPath); err == nil && !fi.IsDir() {
				if fi.ModTime().Before(cutoff) {
					freedBytes += fi.Size()
					if err := os.Remove(jobLogPath); err == nil {
						deletedCount++
					}
				}
			}
		}
	}

	// Compute remaining size
	stats, _ := tl.getStorageStatsUnsafe()

	msg := fmt.Sprintf("Deleted %d log files older than %s, freeing %s.",
		deletedCount, cutoffDateStr, FormatByteSize(freedBytes))

	return DeleteResult{
		DeletedFilesCount:       deletedCount,
		FreedBytes:              freedBytes,
		FreedBytesFormatted:     FormatByteSize(freedBytes),
		RemainingBytes:          stats.TotalSizeBytes,
		RemainingBytesFormatted: stats.TotalSizeFormatted,
		Message:                 msg,
	}, nil
}

// ClearAllLogs purges all central stackdriver log files and all job telemetry files,
// resetting log storage to a clean baseline.
func (tl *TelemetryLogger) ClearAllLogs() (DeleteResult, error) {
	tl.mu.Lock()
	defer tl.mu.Unlock()

	var deletedCount int
	var freedBytes int64

	// 1. Remove all files in GlobalLogsDir (preserving .gitkeep)
	if entries, err := os.ReadDir(tl.GlobalLogsDir); err == nil {
		for _, e := range entries {
			if e.IsDir() || e.Name() == ".gitkeep" {
				continue
			}
			filePath := filepath.Join(tl.GlobalLogsDir, e.Name())
			if fi, err := os.Stat(filePath); err == nil {
				freedBytes += fi.Size()
			}
			if err := os.Remove(filePath); err == nil {
				deletedCount++
			}
		}
	}

	// 2. Remove all job_telemetry.log files in workspace/jobs/*/
	if jobEntries, err := os.ReadDir(tl.JobsDir); err == nil {
		for _, j := range jobEntries {
			if !j.IsDir() {
				continue
			}
			jobLogPath := filepath.Join(tl.JobsDir, j.Name(), "job_telemetry.log")
			if fi, err := os.Stat(jobLogPath); err == nil && !fi.IsDir() {
				freedBytes += fi.Size()
				if err := os.Remove(jobLogPath); err == nil {
					deletedCount++
				}
			}
		}
	}

	// 3. Write clean reset entry
	now := time.Now()
	dateStr := now.Format("2006-01-02")
	resetPath := filepath.Join(tl.GlobalLogsDir, fmt.Sprintf("stackdriver_%s.log", dateStr))
	resetEntry := fmt.Sprintf("[%s] [INFO] [SYSTEM] 🧹 All historical telemetry logs cleared by operator.\n", now.Format("2006-01-02 15:04:05.000"))
	os.WriteFile(resetPath, []byte(resetEntry), 0644)

	stats, _ := tl.getStorageStatsUnsafe()

	msg := fmt.Sprintf("All historical logs cleared (%d files removed, %s freed).",
		deletedCount, FormatByteSize(freedBytes))

	return DeleteResult{
		DeletedFilesCount:       deletedCount,
		FreedBytes:              freedBytes,
		FreedBytesFormatted:     FormatByteSize(freedBytes),
		RemainingBytes:          stats.TotalSizeBytes,
		RemainingBytesFormatted: stats.TotalSizeFormatted,
		Message:                 msg,
	}, nil
}

func (tl *TelemetryLogger) getStorageStatsUnsafe() (LogStorageStats, error) {
	var totalBytes int64
	var globalCount int
	var jobCount int

	if entries, err := os.ReadDir(tl.GlobalLogsDir); err == nil {
		for _, e := range entries {
			if !e.IsDir() && strings.HasSuffix(e.Name(), ".log") {
				globalCount++
				if info, err := e.Info(); err == nil {
					totalBytes += info.Size()
				}
			}
		}
	}

	if jobEntries, err := os.ReadDir(tl.JobsDir); err == nil {
		for _, j := range jobEntries {
			if j.IsDir() {
				jobLogPath := filepath.Join(tl.JobsDir, j.Name(), "job_telemetry.log")
				if fi, err := os.Stat(jobLogPath); err == nil && !fi.IsDir() {
					jobCount++
					totalBytes += fi.Size()
				}
			}
		}
	}

	usagePercent := (float64(totalBytes) / float64(MaxLogStorageBytes)) * 100.0
	if usagePercent > 100.0 {
		usagePercent = 100.0
	}

	return LogStorageStats{
		TotalSizeBytes:     totalBytes,
		TotalSizeFormatted: FormatByteSize(totalBytes),
		MaxSizeBytes:       MaxLogStorageBytes,
		MaxSizeFormatted:   "1.00 GB",
		UsagePercent:       usagePercent,
		IsNearThreshold:    totalBytes >= (MaxLogStorageBytes * 80 / 100),
		GlobalLogCount:     globalCount,
		JobLogCount:        jobCount,
	}, nil
}
