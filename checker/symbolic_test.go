package checker

import (
	"strings"
	"testing"
)

func TestSymbolicRunsAreCountedFromTheClockColumn(t *testing.T) {
	runs := `SELECT 0::BIGINT AS run_id, '{"seed":1,"time":{"mode":"symbolic"}}' AS clock
		UNION ALL SELECT 1::BIGINT, '{"seed":2}'
		UNION ALL SELECT 2::BIGINT, NULL`
	dir := parquetOutput(t, "", "", runs)
	n, err := SymbolicRuns(dir)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("counted %d symbolic runs, want 1", n)
	}
	if w := SymbolicWarning(dir); !strings.Contains(w, "candidate") {
		t.Fatalf("warning %q does not say a violation is a candidate", w)
	}
}

func TestConcreteOutputsGiveNoSymbolicWarning(t *testing.T) {
	concrete := parquetOutput(t, "", "", `SELECT 0::BIGINT AS run_id, '{"seed":1}' AS clock`)
	if w := SymbolicWarning(concrete); w != "" {
		t.Fatalf("a concrete output warned: %q", w)
	}
	if w := SymbolicWarning(parquetOutput(t, "", "", "")); w != "" {
		t.Fatalf("an output with no runs table warned: %q", w)
	}
}
