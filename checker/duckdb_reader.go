package checker

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/marcboeker/go-duckdb"
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
	if isParquetDir(path) {
		// In-memory DuckDB – queries will use read_parquet() inline.
		return sql.Open("duckdb", "")
	}
	return sql.Open("duckdb", path)
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

// ReadEventsFromDuckDB reads execution events for a given run_id.
// Works with both a .duckdb file and a Parquet directory.
func ReadEventsFromDuckDB(dbPath string, runID int) ([]*EventRow, error) {
	db, err := openDB(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	src := executionsSource(dbPath)
	query := fmt.Sprintf(`
		SELECT unique_id, client_id, kind, action, payload
		FROM %s
		WHERE run_id = ?
		ORDER BY seq_num ASC
	`, src)

	rows, err := db.Query(query, runID)
	if err != nil {
		return nil, fmt.Errorf("failed to query executions: %w", err)
	}
	defer rows.Close()

	var eventRows []*EventRow

	for rows.Next() {
		var uniqueID, clientID int
		var kind, action, payload string

		if err := rows.Scan(&uniqueID, &clientID, &kind, &action, &payload); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		var actionType ActionType
		if err := actionType.UnmarshalCSV(action); err != nil {
			log.Printf("Warning: failed to parse action type %q: %v", action, err)
			continue
		}

		eventRows = append(eventRows, &EventRow{
			UniqueID: fmt.Sprintf("%d", uniqueID),
			ClientID: fmt.Sprintf("%d", clientID),
			Kind:     kind,
			Action:   actionType,
			Payload:  payload,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return eventRows, nil
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
