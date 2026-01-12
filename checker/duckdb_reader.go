package checker

import (
	"database/sql"
	"errors"
	"fmt"
	"log"

	_ "github.com/marcboeker/go-duckdb"
)

var (
	ErrNoRunID = errors.New("run_id not found")
)

// ReadEventsFromDuckDB reads execution events from a DuckDB database for a given run_id
// and returns them as EventRow structs that can be used with BuildOperations
func ReadEventsFromDuckDB(dbPath string, runID int) ([]*EventRow, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	query := `
		SELECT unique_id, client_id, kind, action, payload
		FROM executions
		WHERE run_id = ?
		ORDER BY seq_num ASC
	`

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

// ListRunIDs returns all available run IDs from the database
func ListRunIDs(dbPath string) ([]int, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	query := `SELECT run_id FROM runs ORDER BY run_id ASC`

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

// GetRunMetadata retrieves metadata about a specific run
func GetRunMetadata(dbPath string, runID int) (startTime, metaInfo string, err error) {
	db, err := sql.Open("duckdb", dbPath)
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
