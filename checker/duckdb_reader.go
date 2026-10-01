package checker

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	_ "github.com/marcboeker/go-duckdb/v2"
)

var (
	ErrNoRunID = errors.New("run_id not found")
)

// isParquetDir returns true if the given path is a Parquet directory.
// It handles both the root output dir (containing "executions") and the "executions" dir itself.
func isParquetDir(path string) bool {
	info, err := os.Stat(path)
	if err != nil || !info.IsDir() {
		return false
	}

	// Check if this is the root dir (has "executions" subdirectory)
	subDirInfo, err := os.Stat(filepath.Join(path, "executions"))
	if err == nil && subDirInfo.IsDir() {
		return true
	}

	// Check if this is the executions dir itself (contains .parquet files)
	files, err := filepath.Glob(filepath.Join(path, "*.parquet"))
	return err == nil && len(files) > 0
}

// openDB opens an in-memory DuckDB when path is a Parquet directory, or opens
// the DuckDB file directly otherwise.
func openDB(path string) (*sql.DB, error) {
	opts := duckDBDSNOptions()
	if isParquetDir(path) {
		// In-memory DuckDB – queries will use read_parquet() inline.
		return sql.Open("duckdb", opts)
	}
	return sql.Open("duckdb", path+opts)
}

// executionsSource returns the SQL table expression for the executions relation.
func executionsSource(path string) string {
	if isParquetDir(path) {
		// If path is already the executions/ dir
		if filepath.Base(path) == "executions" {
			return fmt.Sprintf("read_parquet('%s', union_by_name=true)", filepath.Join(path, "*.parquet"))
		}
		// If path is the parent dir
		return fmt.Sprintf("read_parquet('%s', union_by_name=true)", filepath.Join(path, "executions", "*.parquet"))
	}
	return "executions"
}

// runsSource returns the SQL table expression for the runs relation.
// For Parquet mode there is no runs file; we synthesise distinct run_ids from executions.
func runsSource(path string) string {
	if isParquetDir(path) {
		// If path is already the executions/ dir
		if filepath.Base(path) == "executions" {
			return fmt.Sprintf(
				"(SELECT DISTINCT run_id FROM read_parquet('%s', union_by_name=true))",
				filepath.Join(path, "*.parquet"),
			)
		}
		// If path is the parent dir
		return fmt.Sprintf(
			"(SELECT DISTINCT run_id FROM read_parquet('%s', union_by_name=true))",
			filepath.Join(path, "executions", "*.parquet"),
		)
	}
	return "runs"
}

// timeColumns returns the select expressions for step and global time:
// the columns when the executions relation has them, else zero.
func timeColumns(db *sql.DB, src string) (string, error) {
	ok, err := hasColumns(db, src, "step", "global_time")
	if err != nil {
		return "", fmt.Errorf("failed to read the executions columns: %w", err)
	}
	if !ok {
		return "0::BIGINT AS step, 0::BIGINT AS global_time", nil
	}
	return "coalesce(step, 0)::BIGINT AS step, coalesce(global_time, 0)::BIGINT AS global_time", nil
}

func scanEvent(rows *sql.Rows, runID *int) (*EventRow, error) {
	var uniqueID, clientID, step, globalTime int64
	var kind, action, payload string
	dest := []interface{}{&uniqueID, &clientID, &kind, &action, &payload, &step, &globalTime}
	if runID != nil {
		dest = append([]interface{}{runID}, dest...)
	}
	if err := rows.Scan(dest...); err != nil {
		return nil, fmt.Errorf("failed to scan row: %w", err)
	}
	return &EventRow{
		UniqueID:   strconv.FormatInt(uniqueID, 10),
		ClientID:   strconv.FormatInt(clientID, 10),
		Kind:       kind,
		Action:     action,
		Payload:    payload,
		Step:       step,
		GlobalTime: globalTime,
	}, nil
}

// ReadEventsFromDuckDB reads every execution row of a run, in row order.
// Works with both a .duckdb file and a Parquet directory.
func ReadEventsFromDuckDB(dbPath string, runID int) ([]*EventRow, error) {
	db, err := openDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	src := executionsSource(dbPath)
	times, err := timeColumns(db, src)
	if err != nil {
		return nil, err
	}
	query := fmt.Sprintf(`
		SELECT unique_id, client_id, kind, action, payload, %s
		FROM %s
		WHERE run_id = ?
		ORDER BY seq_num ASC
	`, times, src)

	rows, err := db.Query(query, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to query executions: %w", err)
	}
	defer rows.Close()

	var eventRows []*EventRow
	for rows.Next() {
		e, err := scanEvent(rows, nil)
		if err != nil {
			return nil, err
		}
		eventRows = append(eventRows, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}
	return eventRows, nil
}

// ProcessAllRunsFromDuckDB executes a single query over all runs, sorted by
// run_id and seq_num, and streams events grouped by run_id to the callback.
func ProcessAllRunsFromDuckDB(dbPath string, processRun func(runID int, events []*EventRow) error) error {
	return processAllRunsExcluding(dbPath, "", processRun)
}

func processAllRunsExcluding(dbPath, excludePasses string, processRun func(runID int, events []*EventRow) error) error {
	db, err := openDB(dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	src := executionsSource(dbPath)
	times, err := timeColumns(db, src)
	if err != nil {
		return err
	}
	exclusion := ""
	if excludePasses != "" {
		exclusion = " AND run_id NOT IN (" + excludePasses + ")"
	}
	// The check consumes invocations and responses, and the view draws
	// crashes and recoveries. Other system rows - timer firings, partitions,
	// clock advances - can outnumber client operations many times over, so
	// the reader keeps only those four kinds and every later system kind is
	// skipped without another edit here.
	query := fmt.Sprintf(`
		SELECT run_id, unique_id, client_id, kind, action, payload, %s
		FROM %s
		WHERE kind IN ('Invocation', 'Response', 'Crash', 'Recover') %s
		ORDER BY run_id ASC, seq_num ASC
	`, times, src, exclusion)

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to query executions: %w", err)
	}
	defer rows.Close()

	var (
		currentRunID = -1
		currentBatch []*EventRow
	)

	flush := func() error {
		if currentBatch != nil {
			if err := processRun(currentRunID, currentBatch); err != nil {
				return err
			}
		}
		return nil
	}

	for rows.Next() {
		var runID int
		e, err := scanEvent(rows, &runID)
		if err != nil {
			return err
		}
		if runID != currentRunID {
			if err := flush(); err != nil {
				return err
			}
			currentRunID = runID
			currentBatch = nil
		}
		currentBatch = append(currentBatch, e)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	// Flush final batch
	return flush()
}

// ListRunIDs returns all available run IDs from the database or Parquet directory.
func ListRunIDs(dbPath string) ([]int, error) {
	db, err := openDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	src := runsSource(dbPath)
	query := fmt.Sprintf(`SELECT run_id FROM %s ORDER BY run_id ASC`, src)

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query runs: %w", err)
	}
	defer rows.Close()

	var runIDs []int
	for rows.Next() {
		var runID int
		if err := rows.Scan(&runID); err != nil {
			return nil, fmt.Errorf("failed to scan run_id: %w", err)
		}
		runIDs = append(runIDs, runID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return runIDs, nil
}

// GetRunMetadata retrieves metadata about a specific run.
// In Parquet mode, start_time and meta_info are not stored, so empty strings are returned.
func GetRunMetadata(dbPath string, runID int) (startTime, metaInfo string, err error) {
	if isParquetDir(dbPath) {
		// Parquet backend doesn't persist run metadata.
		return "", "", nil
	}

	db, err := openDB(dbPath)
	if err != nil {
		return "", "", fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	query := `SELECT start_time, COALESCE(meta_info, '') FROM runs WHERE run_id = ?`

	err = db.QueryRow(query, runID).Scan(&startTime, &metaInfo)
	if err == sql.ErrNoRows {
		return "", "", fmt.Errorf("run_id %d not found: %w", runID, ErrNoRunID)
	}
	if err != nil {
		return "", "", fmt.Errorf("failed to query run metadata: %w", err)
	}

	return startTime, metaInfo, nil
}
