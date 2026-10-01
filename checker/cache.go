package checker

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/benaepli/turnpike-porcupine/checker/engine"
)

const checkerVersion = "spur-engine-1"
const contractVersion = "spur-kv-2"

// CachedVerdict is the simulator's definitive check of one history under the
// claim being checked. Witness is the canonical witness, empty when the
// simulator recorded none.
type CachedVerdict struct {
	Verdict    string
	Triage     string
	Witness    string
	SkippedOps int
}

type CheckCache struct {
	Runs          map[int]CachedVerdict
	excludePasses string
}

func sqlString(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func hasColumns(db *sql.DB, source string, wanted ...string) (bool, error) {
	rows, err := db.Query("SELECT * FROM " + source + " LIMIT 0")
	if err != nil {
		return false, err
	}
	defer rows.Close()
	names, err := rows.Columns()
	if err != nil {
		return false, err
	}
	found := make(map[string]bool)
	for _, name := range names {
		found[name] = true
	}
	for _, name := range wanted {
		if !found[name] {
			return false, nil
		}
	}
	return true, nil
}

// LoadCheckCache reads the simulator's verdicts that may stand in for a
// check under model and claim, the claim in its canonical JSON form.
func LoadCheckCache(dbPath, model, claim string) (*CheckCache, error) {
	cache := &CheckCache{Runs: make(map[int]CachedVerdict)}
	root := dbPath
	if filepath.Base(root) == "executions" {
		root = filepath.Dir(root)
	}
	if info, err := os.Stat(root); err == nil && !info.IsDir() {
		return cache, nil
	}
	bytes, err := os.ReadFile(filepath.Join(root, "checking.json"))
	if os.IsNotExist(err) {
		return cache, nil
	}
	if err != nil {
		return nil, err
	}
	var manifest struct {
		SessionID string   `json:"session_id"`
		Checker   string   `json:"checker_version"`
		Contract  string   `json:"contract_version"`
		Reusable  bool     `json:"reusable"`
		Errors    []string `json:"errors"`
	}
	if err := json.Unmarshal(bytes, &manifest); err != nil {
		return nil, fmt.Errorf("checking manifest: %w", err)
	}
	if len(manifest.Errors) > 0 {
		return nil, fmt.Errorf("corpus persistence failed: %s", strings.Join(manifest.Errors, "; "))
	}
	if !manifest.Reusable || manifest.SessionID == "" || manifest.Checker != checkerVersion || manifest.Contract != contractVersion {
		return cache, nil
	}
	db, err := openDB(dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	checks, present, err := tableSource(db, dbPath, "checks")
	if err != nil || !present {
		return cache, err
	}
	runs, present, err := tableSource(db, dbPath, "runs")
	if err != nil || !present {
		return cache, err
	}
	for _, spec := range []struct {
		source string
		names  []string
	}{
		{checks, []string{"session_id", "run_id", "deployment_id", "history_digest", "model", "claim", "checker_version", "contract_version", "verdict", "triage", "witness", "skipped_ops"}},
		{runs, []string{"run_id", "deployment_id", "check_session_id", "history_digest"}},
	} {
		ok, err := hasColumns(db, spec.source, spec.names...)
		if err != nil || !ok {
			return cache, err
		}
	}
	eligible := fmt.Sprintf(`SELECT c.run_id, c.verdict, c.skipped_ops, coalesce(c.triage, '') AS triage,
        coalesce(c.witness, '') AS witness FROM %s c JOIN %s r
        ON c.run_id = r.run_id AND c.deployment_id = r.deployment_id
        AND c.session_id = r.check_session_id AND c.history_digest = r.history_digest
        WHERE c.session_id = %s AND c.model = %s AND c.claim = %s AND c.checker_version = %s
        AND c.contract_version = %s AND c.history_digest <> '' AND c.verdict IN ('ok', 'illegal')`,
		checks, runs, sqlString(manifest.SessionID), sqlString(model), sqlString(claim), sqlString(checkerVersion), sqlString(contractVersion))
	consistent := "SELECT run_id, min(verdict) AS verdict, min(skipped_ops) AS skipped_ops, min(triage) AS triage, " +
		"min(witness) AS witness FROM (" + eligible + ") GROUP BY run_id HAVING count(DISTINCT verdict) = 1 " +
		"AND count(DISTINCT skipped_ops) = 1 AND count(DISTINCT triage) = 1 AND count(DISTINCT witness) = 1"
	rows, err := db.Query(consistent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var runID, skipped int
		var verdict, triage, witness string
		if err := rows.Scan(&runID, &verdict, &skipped, &triage, &witness); err != nil {
			return nil, err
		}
		cache.Runs[runID] = CachedVerdict{Verdict: verdict, Triage: triage, Witness: witness, SkippedOps: skipped}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	cache.excludePasses = "SELECT run_id FROM (" + consistent + ") WHERE verdict = 'ok'"
	return cache, nil
}

// Runs with metadata but no client events still belong to the corpus.
func recordedRunIDs(dbPath string) ([]int, error) {
	db, err := openDB(dbPath)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	source, present, err := tableSource(db, dbPath, "runs")
	if err != nil {
		return nil, err
	}
	if !present {
		return ListRunIDs(dbPath)
	}
	rows, err := db.Query("SELECT DISTINCT run_id FROM " + source + " ORDER BY run_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// Passed histories are excluded in SQL; callbacks retain full run-id order.
func ProcessRunsWithCache(dbPath, model, claim string, recheckAll bool, process func(int, []*EventRow, *CachedVerdict, bool) error) error {
	cache, err := LoadCheckCache(dbPath, model, claim)
	if err != nil {
		return err
	}
	ids, err := recordedRunIDs(dbPath)
	if err != nil {
		return err
	}
	sort.Ints(ids)
	index := 0
	emit := func(id int, rows []*EventRow) error {
		if cached, ok := cache.Runs[id]; ok {
			return process(id, rows, &cached, !recheckAll && cached.Verdict == engine.VerdictOK)
		}
		return process(id, rows, nil, false)
	}
	excluded := cache.excludePasses
	if recheckAll {
		excluded = ""
	}
	err = processAllRunsExcluding(dbPath, excluded, func(id int, rows []*EventRow) error {
		for index < len(ids) && ids[index] < id {
			if err := emit(ids[index], nil); err != nil {
				return err
			}
			index++
		}
		if index < len(ids) && ids[index] == id {
			index++
		}
		return emit(id, rows)
	})
	if err != nil {
		return err
	}
	for ; index < len(ids); index++ {
		if err := emit(ids[index], nil); err != nil {
			return err
		}
	}
	return nil
}

// ReconcileVerdict joins a fresh check with the simulator's cached one. A
// definitive verdict or triage that contradicts the cache, or a witness that
// differs from a cached one, is a checker disagreement. A cached violation
// stands when the fresh check is undecided.
func ReconcileVerdict(runID int, actual Outcome, cached *CachedVerdict) (Outcome, error) {
	if cached == nil {
		return actual, nil
	}
	if actual.Verdict != engine.VerdictUnknown && actual.Verdict != cached.Verdict {
		return actual, fmt.Errorf("checker disagreement on run %d: Rust=%s Go=%s", runID, cached.Verdict, actual.Verdict)
	}
	if definiteTriage(actual.Triage) && definiteTriage(cached.Triage) && actual.Triage != cached.Triage {
		return actual, fmt.Errorf("checker disagreement on run %d: Rust triage=%s Go triage=%s", runID, cached.Triage, actual.Triage)
	}
	if actual.Witness != "" && cached.Witness != "" && actual.Witness != cached.Witness {
		return actual, fmt.Errorf("checker disagreement on run %d: Rust witness=%s Go witness=%s", runID, cached.Witness, actual.Witness)
	}
	if cached.Verdict == engine.VerdictIllegal && actual.Verdict == engine.VerdictUnknown {
		out := Outcome{Verdict: engine.VerdictIllegal, Triage: actual.Triage, Witness: cached.Witness}
		if definiteTriage(cached.Triage) {
			out.Triage = cached.Triage
		}
		return out, nil
	}
	return actual, nil
}

func definiteTriage(t string) bool { return t != "" && t != engine.VerdictUnknown }
