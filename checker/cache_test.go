package checker

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/anishathalye/porcupine"
)

func cacheFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	writeParquet(t, db, dir, "runs", `SELECT i::BIGINT AS run_id, 0::INTEGER AS deployment_id,
        'session' AS check_session_id, 'digest' AS history_digest FROM range(1,10) r(i)`)
	writeParquet(t, db, dir, "executions", `SELECT i::BIGINT AS run_id, 0::BIGINT AS seq_num,
        i::INTEGER AS unique_id, i::INTEGER AS client_id, 'Invocation' AS kind, 'Client.Read' AS action,
        'invalid payload that a cached pass must not decode' AS payload FROM range(1,9) r(i)`)
	writeParquet(t, db, dir, "checks", `SELECT i::BIGINT AS run_id, 0::INTEGER AS deployment_id,
        CASE WHEN i=7 THEN 'foreign' ELSE 'session' END AS session_id,
        CASE WHEN i=5 THEN 'different' ELSE 'digest' END AS history_digest,
        CASE WHEN i=6 THEN 'kv_rmw' ELSE 'kv' END AS model,
        CASE WHEN i=8 THEN 'future' ELSE '`+checkerVersion+`' END AS checker_version,
        '`+contractVersion+`' AS contract_version,
        CASE WHEN i=2 THEN 'unknown' WHEN i=3 THEN 'illegal' ELSE 'ok' END AS verdict,
        2::BIGINT AS skipped_ops FROM range(1,10) r(i)
        UNION ALL SELECT 4,0,'session','digest','kv','`+checkerVersion+`','`+contractVersion+`','illegal',2`)
	writeManifest(t, dir, true)
	return dir
}

func writeManifest(t *testing.T, dir string, reusable bool) {
	t.Helper()
	bytes, err := json.Marshal(map[string]interface{}{
		"session_id": "session", "checker_version": checkerVersion, "contract_version": contractVersion, "reusable": reusable,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "checking.json"), bytes, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCacheRequiresMatchingHistoryAndContract(t *testing.T) {
	dir := cacheFixture(t)
	cache, err := LoadCheckCache(dir, "kv")
	if err != nil {
		t.Fatal(err)
	}
	want := map[int]CachedVerdict{1: {porcupine.Ok, 2}, 3: {porcupine.Illegal, 2}, 9: {porcupine.Ok, 2}}
	if !reflect.DeepEqual(cache.Runs, want) {
		t.Fatalf("got %#v", cache.Runs)
	}
	if err := os.WriteFile(filepath.Join(dir, "checks", "incomplete.parquet.tmp"), []byte("unfinished"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCheckCache(dir, "kv"); err != nil {
		t.Fatal(err)
	}
	writeManifest(t, dir, false)
	cache, err = LoadCheckCache(dir, "kv")
	if err != nil || len(cache.Runs) != 0 {
		t.Fatalf("unfinished session reused: %v %v", cache, err)
	}
}

func TestCachedPassesAvoidHistoryLoadingAndKeepFullRunOrder(t *testing.T) {
	dir := cacheFixture(t)
	for _, audit := range []bool{false, true} {
		var ids, reused []int
		err := ProcessRunsWithCache(dir, "kv", audit, func(id int, rows []*EventRow, cached *CachedVerdict, reuse bool) error {
			ids = append(ids, id)
			if reuse {
				reused = append(reused, id)
				if rows != nil || cached == nil {
					t.Fatal("cached pass loaded events")
				}
			} else if id < 9 && len(rows) != 1 {
				t.Fatalf("run %d did not load its history", id)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(ids, []int{1, 2, 3, 4, 5, 6, 7, 8, 9}) {
			t.Fatalf("run order %v", ids)
		}
		if audit && len(reused) != 0 {
			t.Fatal("audit reused passes")
		}
		if !audit && !reflect.DeepEqual(reused, []int{1, 9}) {
			t.Fatalf("reused %v", reused)
		}
	}
}

func TestLegacyCorpusWithoutCheckMetadataStillLoadsHistories(t *testing.T) {
	dir := parquetOutput(t, "", "", "")
	calls := 0
	err := ProcessRunsWithCache(dir, "kv", false, func(id int, rows []*EventRow, cached *CachedVerdict, reuse bool) error {
		calls++
		if id != 0 || len(rows) != 1 || cached != nil || reuse {
			t.Fatal("legacy history was skipped")
		}
		return nil
	})
	if err != nil || calls != 1 {
		t.Fatalf("calls=%d error=%v", calls, err)
	}
}

func TestCachedViolationSurvivesDiagnosticTimeoutAndDisagreementIsError(t *testing.T) {
	cached := &CachedVerdict{Verdict: porcupine.Illegal}
	result, err := ReconcileVerdict(1, porcupine.Unknown, cached)
	if err != nil || result != porcupine.Illegal {
		t.Fatal(result, err)
	}
	if _, err := ReconcileVerdict(1, porcupine.Ok, cached); err == nil {
		t.Fatal("contradictory verdict accepted")
	}
	result, err = ReconcileVerdict(1, porcupine.Unknown, nil)
	if err != nil || result != porcupine.Unknown {
		t.Fatal(result, err)
	}
}

func TestPersistenceErrorsRejectTheCorpus(t *testing.T) {
	dir := cacheFixture(t)
	manifest := map[string]interface{}{"reusable": false, "errors": []string{"deployment tables: disk full"}}
	bytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "checking.json"), bytes, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCheckCache(dir, "kv"); err == nil {
		t.Fatal("failed persistence was accepted")
	}
}
