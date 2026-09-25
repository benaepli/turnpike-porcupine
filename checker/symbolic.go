package checker

import "fmt"

// SymbolicRuns counts the runs whose runs row carries a time object in its
// clock column: runs taken under symbolic time. It is zero when the output
// has no runs table or no clock column.
func SymbolicRuns(path string) (int, error) {
	db, err := openDB(path)
	if err != nil {
		return 0, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	src, ok, err := tableSource(db, path, "runs")
	if err != nil || !ok {
		return 0, err
	}
	var n int
	query := fmt.Sprintf(`SELECT count(*) FROM %s WHERE json_extract(clock, '$.time') IS NOT NULL`, src)
	if err := db.QueryRow(query).Scan(&n); err != nil {
		// A runs table without a clock column holds no symbolic run.
		return 0, nil
	}
	return n, nil
}

// SymbolicWarning is the warning for an output with symbolic runs, or empty.
// Under symbolic time an illegal history is only a candidate: the simulator
// replays its witness concretely and records the verdict in the checks
// table, which this checker does not read.
func SymbolicWarning(path string) string {
	n, err := SymbolicRuns(path)
	if err != nil || n == 0 {
		return ""
	}
	return fmt.Sprintf("%d runs were taken under symbolic time; a violation found here is a candidate "+
		"until a concrete replay confirms it, and the simulator's verdicts are in the checks table "+
		"(reason confirmed_by_replay)", n)
}
