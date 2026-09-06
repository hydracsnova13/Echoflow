package broker

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
)

type SyncStatus string

const (
	SyncOnline    SyncStatus = "🟢 Synced"
	SyncPending   SyncStatus = "🟡 Pending Push"
	SyncError     SyncStatus = "🔴 Sync Error"
	SyncAuthError SyncStatus = "🔴 GitHub Auth Failed"
)

type GitSyncManager struct {
	ProjectRoot string
	Status      SyncStatus
	LastError   string
	mu          sync.RWMutex
}

func NewGitSyncManager(root string) *GitSyncManager {
	return &GitSyncManager{
		ProjectRoot: root,
		Status:      SyncOnline,
	}
}

func parseGitError(output string, err error) (bool, string) {
	lower := strings.ToLower(output)
	if strings.Contains(lower, "could not read username") ||
		strings.Contains(lower, "authentication failed") ||
		strings.Contains(lower, "invalid username or password") ||
		strings.Contains(lower, "permission denied (publickey)") ||
		strings.Contains(lower, "password authentication was removed") ||
		strings.Contains(lower, "terminal prompts disabled") ||
		strings.Contains(lower, "access denied") ||
		strings.Contains(lower, "not authorized") ||
		strings.Contains(lower, "repository not found") ||
		strings.Contains(lower, "logon failed") {
		return true, "GitHub Login / Auth Failed (Check GitHub Credentials/Token)"
	}

	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "fatal:") || strings.HasPrefix(line, "error:") || strings.HasPrefix(line, "remote:") {
			return false, line
		}
	}

	if err != nil {
		return false, err.Error()
	}
	return false, "Git operation failed"
}

func (gsm *GitSyncManager) GetStatus() string {
	gsm.mu.RLock()
	defer gsm.mu.RUnlock()

	// 🛡️ CRITICAL: Never mask an active error or auth error!
	if gsm.Status == SyncError || gsm.Status == SyncAuthError {
		if gsm.LastError != "" {
			return fmt.Sprintf("%s: %s", gsm.Status, gsm.LastError)
		}
		return string(gsm.Status)
	}

	dictPath := filepath.Join("pipeline", "config", "domain_dictionary.json")
	cmd := exec.Command("git", "-C", gsm.ProjectRoot, "status", dictPath, "-sb")
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}

	out, err := cmd.Output()
	if err == nil {
		outLines := strings.Split(string(out), "\n")
		hasAhead := false
		hasModified := false

		for _, line := range outLines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			if strings.HasPrefix(line, "##") {
				if strings.Contains(line, "[ahead") {
					hasAhead = true
				}
			} else if strings.Contains(line, "domain_dictionary.json") {
				hasModified = true
			}
		}

		if hasAhead || hasModified {
			return string(SyncPending)
		}
		return string(SyncOnline)
	}

	return string(gsm.Status)
}

func (gsm *GitSyncManager) isOnline() bool {
	client := http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("https://github.com")
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200
}

func (gsm *GitSyncManager) clearGitLock() {
	lockPath := filepath.Join(gsm.ProjectRoot, ".git", "index.lock")
	os.Remove(lockPath)
}

// compiledDictShard mirrors the dictionary structure for JSON marshalling
type compiledDictShard struct {
	AsrCorrections         map[string]string `json:"asr_corrections"`
	DomainTerms            map[string]string `json:"domain_terms"`
	AsrStemPatterns        []interface{}     `json:"asr_stem_patterns"`
	SpokenEnglishSmoothing []interface{}     `json:"spoken_english_smoothing"`
	SpokenHindiSmoothing   []interface{}     `json:"spoken_hindi_smoothing"`
	SpokenMarathiSmoothing []interface{}     `json:"spoken_marathi_smoothing"`
}

type shardWithTimestamp struct {
	Timestamp int64
	Data      compiledDictShard
}

type DictDiff struct {
	AddedCorrections   map[string]string
	UpdatedCorrections map[string]string
	AddedTerms         map[string]string
	UpdatedTerms       map[string]string
	AddedPatterns      int
}

func (d *DictDiff) TotalChanges() int {
	return len(d.AddedCorrections) + len(d.UpdatedCorrections) + len(d.AddedTerms) + len(d.UpdatedTerms) + d.AddedPatterns
}

func (d *DictDiff) Summary() string {
	if d.TotalChanges() == 0 {
		return "No changes (already up-to-date)"
	}
	var parts []string
	if n := len(d.AddedCorrections) + len(d.UpdatedCorrections); n > 0 {
		parts = append(parts, fmt.Sprintf("%d ASR corrections", n))
	}
	if n := len(d.AddedTerms) + len(d.UpdatedTerms); n > 0 {
		parts = append(parts, fmt.Sprintf("%d domain terms", n))
	}
	if d.AddedPatterns > 0 {
		parts = append(parts, fmt.Sprintf("%d pattern rules", d.AddedPatterns))
	}
	return strings.Join(parts, ", ")
}

func (d *DictDiff) DetailedLogLines() []string {
	var lines []string
	for k, v := range d.AddedCorrections {
		if len(lines) >= 15 {
			lines = append(lines, fmt.Sprintf("  ... and %d more ASR corrections", len(d.AddedCorrections)-15))
			break
		}
		lines = append(lines, fmt.Sprintf("  + [ASR] '%s' ➔ '%s'", k, v))
	}
	for k, v := range d.UpdatedCorrections {
		if len(lines) >= 20 {
			lines = append(lines, fmt.Sprintf("  ... and %d more updated corrections", len(d.UpdatedCorrections)-20))
			break
		}
		lines = append(lines, fmt.Sprintf("  ~ [ASR Update] '%s' ➔ '%s'", k, v))
	}
	for k, v := range d.AddedTerms {
		if len(lines) >= 30 {
			lines = append(lines, fmt.Sprintf("  ... and %d more terms", len(d.AddedTerms)-30))
			break
		}
		lines = append(lines, fmt.Sprintf("  + [Term] '%s' ➔ '%s'", k, v))
	}
	for k, v := range d.UpdatedTerms {
		if len(lines) >= 35 {
			lines = append(lines, fmt.Sprintf("  ... and %d more updated terms", len(d.UpdatedTerms)-35))
			break
		}
		lines = append(lines, fmt.Sprintf("  ~ [Term Update] '%s' ➔ '%s'", k, v))
	}
	if d.AddedPatterns > 0 {
		lines = append(lines, fmt.Sprintf("  + %d pattern / smoothing rules", d.AddedPatterns))
	}
	return lines
}

func computeDictDiff(base compiledDictShard, target compiledDictShard) DictDiff {
	diff := DictDiff{
		AddedCorrections:   make(map[string]string),
		UpdatedCorrections: make(map[string]string),
		AddedTerms:         make(map[string]string),
		UpdatedTerms:       make(map[string]string),
	}

	for k, v := range target.AsrCorrections {
		kTrim := strings.TrimSpace(k)
		vTrim := strings.TrimSpace(v)
		if kTrim == "" {
			continue
		}
		if oldV, exists := base.AsrCorrections[kTrim]; !exists {
			diff.AddedCorrections[kTrim] = vTrim
		} else if oldV != vTrim {
			diff.UpdatedCorrections[kTrim] = vTrim
		}
	}

	for k, v := range target.DomainTerms {
		kTrim := strings.TrimSpace(k)
		vTrim := strings.TrimSpace(v)
		if kTrim == "" {
			continue
		}
		if oldV, exists := base.DomainTerms[kTrim]; !exists {
			diff.AddedTerms[kTrim] = vTrim
		} else if oldV != vTrim {
			diff.UpdatedTerms[kTrim] = vTrim
		}
	}

	if len(target.AsrStemPatterns) > len(base.AsrStemPatterns) {
		diff.AddedPatterns += len(target.AsrStemPatterns) - len(base.AsrStemPatterns)
	}
	if len(target.SpokenEnglishSmoothing) > len(base.SpokenEnglishSmoothing) {
		diff.AddedPatterns += len(target.SpokenEnglishSmoothing) - len(base.SpokenEnglishSmoothing)
	}
	if len(target.SpokenHindiSmoothing) > len(base.SpokenHindiSmoothing) {
		diff.AddedPatterns += len(target.SpokenHindiSmoothing) - len(base.SpokenHindiSmoothing)
	}
	if len(target.SpokenMarathiSmoothing) > len(base.SpokenMarathiSmoothing) {
		diff.AddedPatterns += len(target.SpokenMarathiSmoothing) - len(base.SpokenMarathiSmoothing)
	}

	return diff
}

// deduplicateMap removes duplicate keys case-insensitively, trimming whitespace
func deduplicateMap(m map[string]string) map[string]string {
	result := make(map[string]string)
	lowerToKey := make(map[string]string)

	for k, v := range m {
		kTrimmed := strings.TrimSpace(k)
		vTrimmed := strings.TrimSpace(v)
		if kTrimmed == "" {
			continue
		}
		kLower := strings.ToLower(kTrimmed)
		if oldKey, exists := lowerToKey[kLower]; exists {
			delete(result, oldKey)
		}
		lowerToKey[kLower] = kTrimmed
		result[kTrimmed] = vTrimmed
	}
	return result
}

// deduplicatePatterns removes duplicate regex/smoothing patterns based on trimmed pattern string
func deduplicatePatterns(list []interface{}) []interface{} {
	seen := make(map[string]bool)
	var result []interface{}

	for _, item := range list {
		if itemMap, ok := item.(map[string]interface{}); ok {
			pattern, _ := itemMap["pattern"].(string)
			pTrimmed := strings.TrimSpace(pattern)
			if pTrimmed == "" {
				continue
			}
			pKey := strings.ToLower(pTrimmed)
			if !seen[pKey] {
				seen[pKey] = true
				result = append(result, item)
			}
		} else {
			result = append(result, item)
		}
	}
	return result
}

// CompileDictionary reads the base domain_dictionary.json + all shards, merges them with deduplication,
// and writes the compiled result back to domain_dictionary.json.
func (gsm *GitSyncManager) CompileDictionary(logToUI func(string)) error {
	basePath := filepath.Join(gsm.ProjectRoot, "pipeline", "config", "domain_dictionary.json")
	shardsDir := filepath.Join(gsm.ProjectRoot, "pipeline", "config", "shards")

	backupPath := basePath + ".bak"
	if baseBytes, err := os.ReadFile(basePath); err == nil {
		os.WriteFile(backupPath, baseBytes, 0644)
	}

	compiled := compiledDictShard{
		AsrCorrections:         make(map[string]string),
		DomainTerms:            make(map[string]string),
		AsrStemPatterns:        make([]interface{}, 0),
		SpokenEnglishSmoothing: make([]interface{}, 0),
		SpokenHindiSmoothing:   make([]interface{}, 0),
		SpokenMarathiSmoothing: make([]interface{}, 0),
	}

	// Load existing base dictionary
	if baseBytes, err := os.ReadFile(backupPath); err == nil {
		var baseData compiledDictShard
		if json.Unmarshal(baseBytes, &baseData) == nil {
			for k, v := range baseData.AsrCorrections {
				compiled.AsrCorrections[k] = v
			}
			for k, v := range baseData.DomainTerms {
				compiled.DomainTerms[k] = v
			}
			if len(baseData.AsrStemPatterns) > 0 {
				compiled.AsrStemPatterns = append(compiled.AsrStemPatterns, baseData.AsrStemPatterns...)
			}
			if len(baseData.SpokenEnglishSmoothing) > 0 {
				compiled.SpokenEnglishSmoothing = append(compiled.SpokenEnglishSmoothing, baseData.SpokenEnglishSmoothing...)
			}
			if len(baseData.SpokenHindiSmoothing) > 0 {
				compiled.SpokenHindiSmoothing = append(compiled.SpokenHindiSmoothing, baseData.SpokenHindiSmoothing...)
			}
			if len(baseData.SpokenMarathiSmoothing) > 0 {
				compiled.SpokenMarathiSmoothing = append(compiled.SpokenMarathiSmoothing, baseData.SpokenMarathiSmoothing...)
			}
		}
	}

	// Read and sort all shards
	entries, _ := os.ReadDir(shardsDir)
	var shards []shardWithTimestamp

	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		shardBytes, err := os.ReadFile(filepath.Join(shardsDir, entry.Name()))
		if err != nil {
			continue
		}

		var rawShard map[string]interface{}
		if json.Unmarshal(shardBytes, &rawShard) != nil {
			continue
		}

		timestamp := int64(0)
		if meta, ok := rawShard["_meta"].(map[string]interface{}); ok {
			if ts, ok := meta["last_updated"].(float64); ok {
				timestamp = int64(ts)
			}
		}

		var shard compiledDictShard
		if json.Unmarshal(shardBytes, &shard) == nil {
			shards = append(shards, shardWithTimestamp{Timestamp: timestamp, Data: shard})
		}
	}

	sort.Slice(shards, func(i, j int) bool {
		return shards[i].Timestamp < shards[j].Timestamp
	})

	for _, s := range shards {
		for k, v := range s.Data.AsrCorrections {
			compiled.AsrCorrections[k] = v
		}
		for k, v := range s.Data.DomainTerms {
			compiled.DomainTerms[k] = v
		}
		if len(s.Data.AsrStemPatterns) > 0 {
			compiled.AsrStemPatterns = append(compiled.AsrStemPatterns, s.Data.AsrStemPatterns...)
		}
		if len(s.Data.SpokenEnglishSmoothing) > 0 {
			compiled.SpokenEnglishSmoothing = append(compiled.SpokenEnglishSmoothing, s.Data.SpokenEnglishSmoothing...)
		}
		if len(s.Data.SpokenHindiSmoothing) > 0 {
			compiled.SpokenHindiSmoothing = append(compiled.SpokenHindiSmoothing, s.Data.SpokenHindiSmoothing...)
		}
		if len(s.Data.SpokenMarathiSmoothing) > 0 {
			compiled.SpokenMarathiSmoothing = append(compiled.SpokenMarathiSmoothing, s.Data.SpokenMarathiSmoothing...)
		}
	}

	// Deduplicate
	compiled.AsrCorrections = deduplicateMap(compiled.AsrCorrections)
	compiled.DomainTerms = deduplicateMap(compiled.DomainTerms)
	compiled.AsrStemPatterns = deduplicatePatterns(compiled.AsrStemPatterns)
	compiled.SpokenEnglishSmoothing = deduplicatePatterns(compiled.SpokenEnglishSmoothing)
	compiled.SpokenHindiSmoothing = deduplicatePatterns(compiled.SpokenHindiSmoothing)
	compiled.SpokenMarathiSmoothing = deduplicatePatterns(compiled.SpokenMarathiSmoothing)

	compiledBytes, err := json.MarshalIndent(compiled, "", "  ")
	if err != nil {
		if logToUI != nil {
			logToUI(fmt.Sprintf("🔴 [SyncManager] Failed to marshal compiled dictionary: %s", err))
		}
		return err
	}

	if err := os.WriteFile(basePath, compiledBytes, 0644); err != nil {
		if logToUI != nil {
			logToUI(fmt.Sprintf("🔴 [SyncManager] Failed to write compiled dictionary: %s", err))
		}
		return err
	}

	if logToUI != nil {
		logToUI(fmt.Sprintf("📖 [SyncManager] Domain dictionary compiled: %d ASR corrections, %d domain terms, %d stem patterns",
			len(compiled.AsrCorrections), len(compiled.DomainTerms), len(compiled.AsrStemPatterns)))
	}
	return nil
}

func (gsm *GitSyncManager) getMachineID() string {
	idPath := filepath.Join(gsm.ProjectRoot, ".machine_id")
	if bytes, err := os.ReadFile(idPath); err == nil {
		return strings.TrimSpace(string(bytes))
	}
	return ""
}

// fetchRemoteConfigSync fetches origin/main and pulls down remote domain_dictionary.json
// strictly restricted to pipeline/config/domain_dictionary.json (shards are local and gitignored).
func (gsm *GitSyncManager) fetchRemoteConfigSync(machineID string, logToUI func(string)) error {
	gsm.clearGitLock()

	// 1. Fetch remote tracking refs (fails immediately if auth fails without hanging)
	cmdFetch := exec.Command("git", "-C", gsm.ProjectRoot, "fetch", "origin", "main")
	cmdFetch.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	cmdFetch.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if fetchOut, err := cmdFetch.CombinedOutput(); err != nil {
		isAuth, errMsg := parseGitError(string(fetchOut), err)
		gsm.mu.Lock()
		if isAuth {
			gsm.Status = SyncAuthError
			gsm.LastError = errMsg
		} else {
			gsm.Status = SyncError
			gsm.LastError = errMsg
		}
		gsm.mu.Unlock()
		if logToUI != nil {
			logToUI(fmt.Sprintf("🔴 [SyncManager] Git Fetch Error: %s", string(fetchOut)))
		}
		return fmt.Errorf("%s", gsm.LastError)
	}

	// 2. Checkout remote domain_dictionary.json
	dictRelPath := filepath.Join("pipeline", "config", "domain_dictionary.json")
	cmdCheckoutDict := exec.Command("git", "-C", gsm.ProjectRoot, "checkout", "origin/main", "--", dictRelPath)
	cmdCheckoutDict.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	cmdCheckoutDict.CombinedOutput()

	// 3. Unstage pipeline/config so nothing is left staged in git index
	cmdReset := exec.Command("git", "-C", gsm.ProjectRoot, "reset", "HEAD", "--", "pipeline/config")
	cmdReset.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	cmdReset.CombinedOutput()

	return nil
}

// SyncLocal updates domain_dictionary.json with the changes from the user's local shard,
// and stages/commits/pushes ONLY domain_dictionary.json.
// The user's local shard remains private and gitignored on disk, never committed or visible to others.
func (gsm *GitSyncManager) SyncLocal(machineID string, logToUI func(string)) (string, error) {
	if machineID == "" {
		machineID = gsm.getMachineID()
	}

	shardFile := fmt.Sprintf("dict_%s.json", machineID)
	shardRelPath := filepath.Join("pipeline", "config", "shards", shardFile)
	shardAbsPath := filepath.Join(gsm.ProjectRoot, shardRelPath)
	dictRelPath := filepath.Join("pipeline", "config", "domain_dictionary.json")
	dictAbsPath := filepath.Join(gsm.ProjectRoot, dictRelPath)

	// 1. Read local shard
	shardBytes, err := os.ReadFile(shardAbsPath)
	if err != nil {
		msg := fmt.Sprintf("Local shard %s not found on disk: %v", shardFile, err)
		if logToUI != nil {
			logToUI("⚠️ [SyncManager] " + msg)
		}
		gsm.mu.Lock()
		gsm.Status = SyncError
		gsm.LastError = msg
		gsm.mu.Unlock()
		return msg, err
	}

	var localShard compiledDictShard
	if err := json.Unmarshal(shardBytes, &localShard); err != nil {
		msg := fmt.Sprintf("Invalid local shard JSON: %v", err)
		if logToUI != nil {
			logToUI("🔴 [SyncManager] " + msg)
		}
		gsm.mu.Lock()
		gsm.Status = SyncError
		gsm.LastError = msg
		gsm.mu.Unlock()
		return msg, err
	}

	// 2. Read existing domain_dictionary.json
	var currentDict compiledDictShard
	if dictBytes, err := os.ReadFile(dictAbsPath); err == nil {
		json.Unmarshal(dictBytes, &currentDict)
	}
	if currentDict.AsrCorrections == nil {
		currentDict.AsrCorrections = make(map[string]string)
	}
	if currentDict.DomainTerms == nil {
		currentDict.DomainTerms = make(map[string]string)
	}

	// Clone currentDict as preMerge base for diffing
	var preMergeDict compiledDictShard
	preMergeBytes, _ := json.Marshal(currentDict)
	json.Unmarshal(preMergeBytes, &preMergeDict)

	// 3. Overlay local shard changes into domain_dictionary.json
	for k, v := range localShard.AsrCorrections {
		currentDict.AsrCorrections[k] = v
	}
	for k, v := range localShard.DomainTerms {
		currentDict.DomainTerms[k] = v
	}
	if len(localShard.AsrStemPatterns) > 0 {
		currentDict.AsrStemPatterns = append(currentDict.AsrStemPatterns, localShard.AsrStemPatterns...)
	}
	if len(localShard.SpokenEnglishSmoothing) > 0 {
		currentDict.SpokenEnglishSmoothing = append(currentDict.SpokenEnglishSmoothing, localShard.SpokenEnglishSmoothing...)
	}
	if len(localShard.SpokenHindiSmoothing) > 0 {
		currentDict.SpokenHindiSmoothing = append(currentDict.SpokenHindiSmoothing, localShard.SpokenHindiSmoothing...)
	}
	if len(localShard.SpokenMarathiSmoothing) > 0 {
		currentDict.SpokenMarathiSmoothing = append(currentDict.SpokenMarathiSmoothing, localShard.SpokenMarathiSmoothing...)
	}

	// Deduplicate before writing
	currentDict.AsrCorrections = deduplicateMap(currentDict.AsrCorrections)
	currentDict.DomainTerms = deduplicateMap(currentDict.DomainTerms)
	currentDict.AsrStemPatterns = deduplicatePatterns(currentDict.AsrStemPatterns)
	currentDict.SpokenEnglishSmoothing = deduplicatePatterns(currentDict.SpokenEnglishSmoothing)
	currentDict.SpokenHindiSmoothing = deduplicatePatterns(currentDict.SpokenHindiSmoothing)
	currentDict.SpokenMarathiSmoothing = deduplicatePatterns(currentDict.SpokenMarathiSmoothing)

	// Compute diff between preMergeDict and currentDict
	diff := computeDictDiff(preMergeDict, currentDict)

	updatedDictBytes, _ := json.MarshalIndent(currentDict, "", "  ")
	if err := os.WriteFile(dictAbsPath, updatedDictBytes, 0644); err != nil {
		msg := fmt.Sprintf("Failed to update domain_dictionary.json: %v", err)
		if logToUI != nil {
			logToUI("🔴 [SyncManager] " + msg)
		}
		gsm.mu.Lock()
		gsm.Status = SyncError
		gsm.LastError = msg
		gsm.mu.Unlock()
		return msg, err
	}

	if logToUI != nil {
		if diff.TotalChanges() > 0 {
			logToUI(fmt.Sprintf("📖 [SyncManager] Local changes merged into Global Domain Dictionary (%s):", diff.Summary()))
			for _, line := range diff.DetailedLogLines() {
				logToUI("   " + line)
			}
		} else {
			logToUI("ℹ️ [SyncManager] Global Domain Dictionary already contains all terms from your local shard (0 changes).")
		}
	}

	// 4. Git operations: Stage and commit ONLY domain_dictionary.json (shard is gitignored)
	gsm.clearGitLock()

	// Unstage any other files in git index
	cmdResetIndex := exec.Command("git", "-C", gsm.ProjectRoot, "reset", "HEAD", "--")
	cmdResetIndex.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	cmdResetIndex.CombinedOutput()

	// Stage ONLY domain_dictionary.json
	cmdAdd := exec.Command("git", "-C", gsm.ProjectRoot, "add", "--", dictRelPath)
	cmdAdd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	if out, err := cmdAdd.CombinedOutput(); err != nil && logToUI != nil {
		logToUI(fmt.Sprintf("🟡 [SyncManager] Git Add Note: %s", string(out)))
	}

	// Commit ONLY domain_dictionary.json
	commitMsg := fmt.Sprintf("Update domain dictionary: %s (%s)", diff.Summary(), machineID)
	cmdCommit := exec.Command("git", "-C", gsm.ProjectRoot, "commit", "-m", commitMsg, "--", dictRelPath)
	cmdCommit.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	cmdCommit.CombinedOutput()

	if !gsm.isOnline() {
		gsm.mu.Lock()
		gsm.Status = SyncPending
		gsm.LastError = "System is offline. Committed locally."
		gsm.mu.Unlock()
		if logToUI != nil {
			logToUI("📡 [SyncManager] Offline: Local changes committed to domain_dictionary.json locally. Push deferred.")
		}
		return fmt.Sprintf("Committed locally (Offline): %s", diff.Summary()), nil
	}

	// Push commit to GitHub origin/main with GIT_TERMINAL_PROMPT=0
	cmdPush := exec.Command("git", "-C", gsm.ProjectRoot, "push", "origin", "main")
	cmdPush.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	cmdPush.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
	pushOut, err := cmdPush.CombinedOutput()

	if err != nil {
		isAuth, authErrMsg := parseGitError(string(pushOut), err)
		if isAuth {
			gsm.mu.Lock()
			gsm.Status = SyncAuthError
			gsm.LastError = authErrMsg
			gsm.mu.Unlock()
			if logToUI != nil {
				logToUI(fmt.Sprintf("🔴 [SyncManager] %s: %s", gsm.Status, string(pushOut)))
			}
			return authErrMsg, fmt.Errorf("%s", authErrMsg)
		}

		// If rejected because remote has changes, use --autostash --rebase
		gsm.clearGitLock()
		cmdRebase := exec.Command("git", "-C", gsm.ProjectRoot, "pull", "--autostash", "--rebase", "origin", "main")
		cmdRebase.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		cmdRebase.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
		rebaseOut, rebaseErr := cmdRebase.CombinedOutput()

		if rebaseErr != nil {
			isAuthRebase, rebaseErrMsg := parseGitError(string(rebaseOut), rebaseErr)
			gsm.mu.Lock()
			if isAuthRebase {
				gsm.Status = SyncAuthError
				gsm.LastError = rebaseErrMsg
			} else {
				gsm.Status = SyncError
				gsm.LastError = rebaseErrMsg
			}
			gsm.mu.Unlock()
			if logToUI != nil {
				logToUI(fmt.Sprintf("🔴 [SyncManager] Git Rebase Error: %s", string(rebaseOut)))
			}
			return gsm.LastError, rebaseErr
		}

		cmdPushRetry := exec.Command("git", "-C", gsm.ProjectRoot, "push", "origin", "main")
		cmdPushRetry.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		cmdPushRetry.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
		pushRetryOut, retryErr := cmdPushRetry.CombinedOutput()

		if retryErr != nil {
			isAuthRetry, retryErrMsg := parseGitError(string(pushRetryOut), retryErr)
			gsm.mu.Lock()
			if isAuthRetry {
				gsm.Status = SyncAuthError
				gsm.LastError = retryErrMsg
			} else {
				gsm.Status = SyncError
				gsm.LastError = retryErrMsg
			}
			gsm.mu.Unlock()
			if logToUI != nil {
				logToUI(fmt.Sprintf("🔴 [SyncManager] Git Push Error: %s", string(pushRetryOut)+string(pushOut)))
			}
			return gsm.LastError, retryErr
		}
	}

	gsm.mu.Lock()
	gsm.Status = SyncOnline
	gsm.LastError = ""
	gsm.mu.Unlock()
	if logToUI != nil {
		if diff.TotalChanges() > 0 {
			logToUI(fmt.Sprintf("🌐 [SyncManager] Successfully pushed to GitHub Global Domain Dictionary (%s)!", diff.Summary()))
		} else {
			logToUI("🌐 [SyncManager] Pushed to GitHub: Global Domain Dictionary is up-to-date with origin/main.")
		}
	}

	var responseParts []string
	if diff.TotalChanges() > 0 {
		responseParts = append(responseParts, fmt.Sprintf("Pushed to GitHub: %s", diff.Summary()))
		responseParts = append(responseParts, diff.DetailedLogLines()...)
	} else {
		responseParts = append(responseParts, "Global Domain Dictionary is up-to-date with your local shard (0 changes).")
	}
	return strings.Join(responseParts, "\n"), nil
}

// SyncGlobal pulls remote domain_dictionary.json strictly from GitHub,
// overlays local shard contributions, pushes them to GitHub if needed,
// and logs in detail everything that was updated and pushed.
// No other files in the repository are ever affected or modified.
func (gsm *GitSyncManager) SyncGlobal(logToUI func(string)) (string, error) {
	if !gsm.isOnline() {
		gsm.mu.Lock()
		gsm.Status = SyncPending
		gsm.LastError = "System is offline"
		gsm.mu.Unlock()
		if logToUI != nil {
			logToUI("📡 [SyncManager] System is offline. Cannot pull from GitHub.")
		}
		return "Offline", fmt.Errorf("system is offline")
	}

	dictRelPath := filepath.Join("pipeline", "config", "domain_dictionary.json")
	dictAbsPath := filepath.Join(gsm.ProjectRoot, dictRelPath)

	// 1. Read existing local domain_dictionary.json before pulling
	var prePullDict compiledDictShard
	if dictBytes, err := os.ReadFile(dictAbsPath); err == nil {
		json.Unmarshal(dictBytes, &prePullDict)
	}

	machineID := gsm.getMachineID()

	// 2. Fetch remote domain_dictionary.json from origin/main
	if err := gsm.fetchRemoteConfigSync(machineID, logToUI); err != nil {
		return gsm.LastError, err
	}

	// 3. Read newly pulled remote domain_dictionary.json
	var remoteDict compiledDictShard
	if dictBytes, err := os.ReadFile(dictAbsPath); err == nil {
		json.Unmarshal(dictBytes, &remoteDict)
	}

	// Compute diff between what we had locally and what came from remote
	pulledDiff := computeDictDiff(prePullDict, remoteDict)

	if logToUI != nil {
		if pulledDiff.TotalChanges() > 0 {
			logToUI(fmt.Sprintf("🌐 [SyncManager] Pulled updates from GitHub into Global Domain Dictionary (%s):", pulledDiff.Summary()))
			for _, line := range pulledDiff.DetailedLogLines() {
				logToUI("   " + line)
			}
		} else {
			logToUI("🌐 [SyncManager] GitHub remote is up-to-date with local base (0 remote updates).")
		}
	}

	// 4. Overlay local shard contributions onto remoteDict
	shardFile := fmt.Sprintf("dict_%s.json", machineID)
	shardRelPath := filepath.Join("pipeline", "config", "shards", shardFile)
	shardAbsPath := filepath.Join(gsm.ProjectRoot, shardRelPath)

	mergedDict := remoteDict
	if mergedDict.AsrCorrections == nil {
		mergedDict.AsrCorrections = make(map[string]string)
	}
	if mergedDict.DomainTerms == nil {
		mergedDict.DomainTerms = make(map[string]string)
	}

	hasLocalShard := false
	if shardBytes, err := os.ReadFile(shardAbsPath); err == nil {
		var localShard compiledDictShard
		if json.Unmarshal(shardBytes, &localShard) == nil {
			hasLocalShard = true
			for k, v := range localShard.AsrCorrections {
				mergedDict.AsrCorrections[k] = v
			}
			for k, v := range localShard.DomainTerms {
				mergedDict.DomainTerms[k] = v
			}
			if len(localShard.AsrStemPatterns) > 0 {
				mergedDict.AsrStemPatterns = append(mergedDict.AsrStemPatterns, localShard.AsrStemPatterns...)
			}
			if len(localShard.SpokenEnglishSmoothing) > 0 {
				mergedDict.SpokenEnglishSmoothing = append(mergedDict.SpokenEnglishSmoothing, localShard.SpokenEnglishSmoothing...)
			}
			if len(localShard.SpokenHindiSmoothing) > 0 {
				mergedDict.SpokenHindiSmoothing = append(mergedDict.SpokenHindiSmoothing, localShard.SpokenHindiSmoothing...)
			}
			if len(localShard.SpokenMarathiSmoothing) > 0 {
				mergedDict.SpokenMarathiSmoothing = append(mergedDict.SpokenMarathiSmoothing, localShard.SpokenMarathiSmoothing...)
			}
		}
	}

	// Deduplicate merged dictionary
	mergedDict.AsrCorrections = deduplicateMap(mergedDict.AsrCorrections)
	mergedDict.DomainTerms = deduplicateMap(mergedDict.DomainTerms)
	mergedDict.AsrStemPatterns = deduplicatePatterns(mergedDict.AsrStemPatterns)
	mergedDict.SpokenEnglishSmoothing = deduplicatePatterns(mergedDict.SpokenEnglishSmoothing)
	mergedDict.SpokenHindiSmoothing = deduplicatePatterns(mergedDict.SpokenHindiSmoothing)
	mergedDict.SpokenMarathiSmoothing = deduplicatePatterns(mergedDict.SpokenMarathiSmoothing)

	// Compute what local shard contributed that was NOT already in remoteDict
	pushedDiff := computeDictDiff(remoteDict, mergedDict)

	// 5. Write final dictionary to disk
	updatedDictBytes, _ := json.MarshalIndent(mergedDict, "", "  ")
	if err := os.WriteFile(dictAbsPath, updatedDictBytes, 0644); err != nil {
		msg := fmt.Sprintf("Failed to update domain_dictionary.json: %v", err)
		if logToUI != nil {
			logToUI("🔴 [SyncManager] " + msg)
		}
		gsm.mu.Lock()
		gsm.Status = SyncError
		gsm.LastError = msg
		gsm.mu.Unlock()
		return msg, err
	}

	// 6. If local shard had new contributions to push, commit and push them!
	if pushedDiff.TotalChanges() > 0 {
		gsm.clearGitLock()

		cmdAdd := exec.Command("git", "-C", gsm.ProjectRoot, "add", dictRelPath)
		cmdAdd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
		cmdAdd.CombinedOutput()

		commitMsg := fmt.Sprintf("Global Sync: %s (%s)", pushedDiff.Summary(), machineID)
		cmdCommit := exec.Command("git", "-C", gsm.ProjectRoot, "commit", "-m", commitMsg, "--", dictRelPath)
		cmdCommit.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
		cmdCommit.CombinedOutput()

		cmdPush := exec.Command("git", "-C", gsm.ProjectRoot, "push", "origin", "main")
		cmdPush.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		cmdPush.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
		pushOut, err := cmdPush.CombinedOutput()

		if err != nil {
			isAuth, authErrMsg := parseGitError(string(pushOut), err)
			if isAuth {
				gsm.mu.Lock()
				gsm.Status = SyncAuthError
				gsm.LastError = authErrMsg
				gsm.mu.Unlock()
				if logToUI != nil {
					logToUI(fmt.Sprintf("🔴 [SyncManager] %s: %s", gsm.Status, string(pushOut)))
				}
				return authErrMsg, fmt.Errorf("%s", authErrMsg)
			}

			// Rebase retry
			gsm.clearGitLock()
			cmdRebase := exec.Command("git", "-C", gsm.ProjectRoot, "pull", "--autostash", "--rebase", "origin", "main")
			cmdRebase.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
			cmdRebase.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
			cmdRebase.CombinedOutput()

			cmdPushRetry := exec.Command("git", "-C", gsm.ProjectRoot, "push", "origin", "main")
			cmdPushRetry.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
			cmdPushRetry.SysProcAttr = &syscall.SysProcAttr{HideWindow: true}
			pushRetryOut, retryErr := cmdPushRetry.CombinedOutput()

			if retryErr != nil {
				isAuthRetry, retryErrMsg := parseGitError(string(pushRetryOut), retryErr)
				gsm.mu.Lock()
				if isAuthRetry {
					gsm.Status = SyncAuthError
					gsm.LastError = retryErrMsg
				} else {
					gsm.Status = SyncError
					gsm.LastError = retryErrMsg
				}
				gsm.mu.Unlock()
				if logToUI != nil {
					logToUI(fmt.Sprintf("🔴 [SyncManager] Git Push Error: %s", string(pushRetryOut)+string(pushOut)))
				}
				return gsm.LastError, retryErr
			}
		}

		if logToUI != nil {
			logToUI(fmt.Sprintf("📤 [SyncManager] Pushed local contributions to GitHub Global Domain Dictionary (%s):", pushedDiff.Summary()))
			for _, line := range pushedDiff.DetailedLogLines() {
				logToUI("   " + line)
			}
		}
	} else if hasLocalShard {
		if logToUI != nil {
			logToUI("ℹ️ [SyncManager] All local shard terms already exist in Global Domain Dictionary (0 terms pushed).")
		}
	}

	gsm.mu.Lock()
	gsm.Status = SyncOnline
	gsm.LastError = ""
	gsm.mu.Unlock()

	// Build human-readable changelog response
	var responseParts []string
	if pulledDiff.TotalChanges() > 0 && pushedDiff.TotalChanges() > 0 {
		responseParts = append(responseParts, fmt.Sprintf("Global Synced: Pulled %s | Pushed %s", pulledDiff.Summary(), pushedDiff.Summary()))
		responseParts = append(responseParts, "\n📥 Pulled from Remote:")
		responseParts = append(responseParts, pulledDiff.DetailedLogLines()...)
		responseParts = append(responseParts, "\n📤 Pushed to Global Dictionary:")
		responseParts = append(responseParts, pushedDiff.DetailedLogLines()...)
	} else if pushedDiff.TotalChanges() > 0 {
		responseParts = append(responseParts, fmt.Sprintf("Pushed to Global Dictionary: %s", pushedDiff.Summary()))
		responseParts = append(responseParts, pushedDiff.DetailedLogLines()...)
	} else if pulledDiff.TotalChanges() > 0 {
		responseParts = append(responseParts, fmt.Sprintf("Pulled from GitHub: %s", pulledDiff.Summary()))
		responseParts = append(responseParts, pulledDiff.DetailedLogLines()...)
	} else {
		responseParts = append(responseParts, "Global Domain Dictionary is up-to-date with GitHub (0 changes).")
	}

	return strings.Join(responseParts, "\n"), nil
}

// UpdateDomainDictionary pulls global dictionary changes and compiles all local shards into domain_dictionary.json.
// No other files in the repository are ever affected or modified.
func (gsm *GitSyncManager) UpdateDomainDictionary(logToUI func(string)) (string, error) {
	dictRelPath := filepath.Join("pipeline", "config", "domain_dictionary.json")
	dictAbsPath := filepath.Join(gsm.ProjectRoot, dictRelPath)

	var preDict compiledDictShard
	if dictBytes, err := os.ReadFile(dictAbsPath); err == nil {
		json.Unmarshal(dictBytes, &preDict)
	}

	if gsm.isOnline() {
		machineID := gsm.getMachineID()
		if err := gsm.fetchRemoteConfigSync(machineID, logToUI); err != nil {
			if logToUI != nil {
				logToUI(fmt.Sprintf("⚠️ [SyncManager] Remote fetch failed (%v). Compiling with local dictionary data.", err))
			}
			gsm.CompileDictionary(logToUI)
			return gsm.LastError, err
		}
	}

	if err := gsm.CompileDictionary(logToUI); err != nil {
		gsm.mu.Lock()
		gsm.Status = SyncError
		gsm.LastError = fmt.Sprintf("Compile Failed: %v", err)
		gsm.mu.Unlock()
		return "Compile Failed", err
	}

	var postDict compiledDictShard
	if dictBytes, err := os.ReadFile(dictAbsPath); err == nil {
		json.Unmarshal(dictBytes, &postDict)
	}

	diff := computeDictDiff(preDict, postDict)

	gsm.mu.Lock()
	gsm.Status = SyncOnline
	gsm.LastError = ""
	gsm.mu.Unlock()

	if logToUI != nil {
		if diff.TotalChanges() > 0 {
			logToUI(fmt.Sprintf("📖 [SyncManager] Global Domain Dictionary updated and consolidated (%s):", diff.Summary()))
			for _, line := range diff.DetailedLogLines() {
				logToUI("   " + line)
			}
		} else {
			logToUI("📖 [SyncManager] Global Domain Dictionary compiled: all local and global entries are up-to-date.")
		}
	}
	return fmt.Sprintf("Domain Dictionary Updated: %s", diff.Summary()), nil
}

// AttemptSync maintains backward compatibility with legacy single-button sync calls
func (gsm *GitSyncManager) AttemptSync(logToUI func(string)) {
	gsm.UpdateDomainDictionary(logToUI)
}
