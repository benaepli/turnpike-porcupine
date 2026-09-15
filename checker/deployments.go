package checker

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// NodeLabel describes one deployed node: its role, its position among the
// nodes of that role, and the path that reaches it from the deployment root
// (empty when no path reaches it).
type NodeLabel struct {
	Role    string
	Ordinal int
	Path    string
}

// Topology maps each run to the node labels of the deployment it ran.
type Topology struct {
	nodes map[int32]map[int]NodeLabel
	runs  map[int]int32
}

// NamesForRun returns the node names of the run's deployment, or nil when the
// run has no known deployment.
func (t *Topology) NamesForRun(runID int) NodeNames {
	if t == nil {
		return nil
	}
	dep, ok := t.runs[runID]
	if !ok {
		return nil
	}
	nodes, ok := t.nodes[dep]
	if !ok {
		return nil
	}
	return func(index int) (NodeLabel, bool) {
		l, ok := nodes[index]
		return l, ok
	}
}

// tableSource returns the SQL table expression for a named table of the
// output at path, and false when the output has no such table. A Parquet
// output keeps each table in a directory of that name next to executions.
func tableSource(db *sql.DB, path, table string) (string, bool, error) {
	if isParquetDir(path) {
		root := path
		if filepath.Base(path) == "executions" {
			root = filepath.Dir(path)
		}
		dir := filepath.Join(root, table)
		files, err := filepath.Glob(filepath.Join(dir, "*.parquet"))
		if err != nil || len(files) == 0 {
			return "", false, nil
		}
		return fmt.Sprintf("read_parquet('%s', union_by_name=true)", filepath.Join(dir, "*.parquet")), true, nil
	}
	if _, err := os.Stat(path); err != nil {
		return "", false, err
	}
	var n int
	if err := db.QueryRow(
		`SELECT count(*) FROM information_schema.tables WHERE table_name = ?`, table,
	).Scan(&n); err != nil {
		return "", false, fmt.Errorf("failed to look up table %s: %w", table, err)
	}
	return table, n > 0, nil
}

// DeploymentModels returns the distinct models recorded in the deployments
// table, sorted, and false when the output has no deployments table.
func DeploymentModels(path string) ([]string, bool, error) {
	db, err := openDB(path)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	src, ok, err := tableSource(db, path, "deployments")
	if err != nil || !ok {
		return nil, false, err
	}
	rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT model FROM %s WHERE model IS NOT NULL`, src))
	if err != nil {
		return nil, true, fmt.Errorf("failed to query deployments: %w", err)
	}
	defer rows.Close()
	var models []string
	for rows.Next() {
		var m string
		if err := rows.Scan(&m); err != nil {
			return nil, true, fmt.Errorf("failed to scan model: %w", err)
		}
		models = append(models, m)
	}
	if err := rows.Err(); err != nil {
		return nil, true, fmt.Errorf("error iterating deployments: %w", err)
	}
	sort.Strings(models)
	return models, true, nil
}

// ResolveModel picks the model to check the output at path with. An empty
// flag takes the one model the deployments table records, and it is an error
// when the table is missing or records no model or several. A non-empty flag
// always wins; the returned warning is non-empty when the flag disagrees with
// what the table records.
func ResolveModel(flagModel, path string) (model, warning string, err error) {
	models, found, err := DeploymentModels(path)
	if flagModel != "" {
		if err != nil || !found || len(models) == 0 {
			return flagModel, "", nil
		}
		if len(models) != 1 || models[0] != flagModel {
			return flagModel, fmt.Sprintf(
				"-model %s differs from the model recorded in the deployments table (%s); using %s",
				flagModel, strings.Join(models, ", "), flagModel), nil
		}
		return flagModel, "", nil
	}
	if err != nil {
		return "", "", fmt.Errorf("cannot read the model from the deployments table: %w", err)
	}
	if !found {
		return "", "", fmt.Errorf("no deployments table in %s; pass -model", path)
	}
	switch len(models) {
	case 0:
		return "", "", fmt.Errorf("the deployments table in %s records no model; pass -model", path)
	case 1:
		return models[0], "", nil
	default:
		return "", "", fmt.Errorf("the deployments table in %s records several models (%s); pass -model",
			path, strings.Join(models, ", "))
	}
}

// ReadTopology reads the deployment_nodes table and the run-to-deployment
// mapping of the runs table. It returns nil without error when the output has
// either table missing, so callers fall back to naming nodes by index.
func ReadTopology(path string) (*Topology, error) {
	db, err := openDB(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	nodesSrc, ok, err := tableSource(db, path, "deployment_nodes")
	if err != nil || !ok {
		return nil, err
	}
	runsSrc, ok, err := tableSource(db, path, "runs")
	if err != nil || !ok {
		return nil, err
	}

	t := &Topology{
		nodes: make(map[int32]map[int]NodeLabel),
		runs:  make(map[int]int32),
	}

	rows, err := db.Query(fmt.Sprintf(
		`SELECT deployment_id, node_index, role, ordinal, path FROM %s`, nodesSrc))
	if err != nil {
		return nil, fmt.Errorf("failed to query deployment_nodes: %w", err)
	}
	for rows.Next() {
		var dep int32
		var index, ordinal int
		var role string
		var nodePath sql.NullString
		if err := rows.Scan(&dep, &index, &role, &ordinal, &nodePath); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan deployment node: %w", err)
		}
		m, ok := t.nodes[dep]
		if !ok {
			m = make(map[int]NodeLabel)
			t.nodes[dep] = m
		}
		m[index] = NodeLabel{Role: role, Ordinal: ordinal, Path: nodePath.String}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("error iterating deployment_nodes: %w", err)
	}
	rows.Close()

	// A run that failed before a deployment was chosen has no nodes to name.
	rows, err = db.Query(fmt.Sprintf(
		`SELECT run_id, deployment_id FROM %s WHERE deployment_id >= 0`, runsSrc))
	if err != nil {
		return nil, fmt.Errorf("failed to query runs: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var runID int
		var dep int32
		if err := rows.Scan(&runID, &dep); err != nil {
			return nil, fmt.Errorf("failed to scan run deployment: %w", err)
		}
		t.runs[runID] = dep
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating runs: %w", err)
	}
	return t, nil
}
