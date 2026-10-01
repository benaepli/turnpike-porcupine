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

// ParseClaimJSON reads a claim in its JSON object form under model m. The
// claim gets a mark for exactly the kinds m declares: a declared kind the
// object leaves out is linearizable, as an unmarked operation is, and a key
// m does not declare is ignored.
func ParseClaimJSON(m engine.Model, s string) (engine.Claim, error) {
	var marks map[string]string
	if err := json.Unmarshal([]byte(s), &marks); err != nil {
		return engine.Claim{}, fmt.Errorf("claim %q: %w", s, err)
	}
	declared := make(map[string]string)
	for _, k := range []engine.Kind{engine.Write, engine.Read, engine.RMW} {
		if !m.Declared().Has(k) {
			continue
		}
		mark, ok := marks[k.String()]
		if !ok {
			mark = engine.Linearizable.String()
		}
		declared[k.String()] = mark
	}
	c, err := engine.ParseClaim(m, declared)
	if err != nil {
		return c, fmt.Errorf("claim %q: %w", s, err)
	}
	return c, nil
}

// DeploymentClaims returns the distinct claims recorded in the deployments
// table, raw and sorted, and false when the output has no deployments table
// or the table has no claim column.
func DeploymentClaims(path string) ([]string, bool, error) {
	db, err := openDB(path)
	if err != nil {
		return nil, false, fmt.Errorf("failed to open database: %w", err)
	}
	defer db.Close()

	src, ok, err := tableSource(db, path, "deployments")
	if err != nil || !ok {
		return nil, false, err
	}
	if ok, err := hasColumns(db, src, "claim"); err != nil || !ok {
		return nil, false, err
	}
	rows, err := db.Query(fmt.Sprintf(`SELECT DISTINCT claim FROM %s WHERE claim IS NOT NULL`, src))
	if err != nil {
		return nil, true, fmt.Errorf("failed to query deployments: %w", err)
	}
	defer rows.Close()
	var claims []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, true, fmt.Errorf("failed to scan claim: %w", err)
		}
		claims = append(claims, c)
	}
	if err := rows.Err(); err != nil {
		return nil, true, fmt.Errorf("error iterating deployments: %w", err)
	}
	sort.Strings(claims)
	return claims, true, nil
}

// ResolveClaim picks the claim to check the output at path with, under model
// m. A non-empty flag, the claim's JSON object, always wins, with a warning
// when it differs from what the deployments table records. Otherwise the one
// claim the table records is used; an empty path, no deployments table, or a
// table without a claim column means every kind is linearizable. Several
// distinct claims are an error.
func ResolveClaim(flagClaim string, m engine.Model, path string) (engine.Claim, string, error) {
	var recorded []engine.Claim
	var readErr error
	if path != "" {
		raw, found, err := DeploymentClaims(path)
		readErr = err
		if err == nil && found {
			seen := make(map[string]bool)
			for _, r := range raw {
				c, err := ParseClaimJSON(m, r)
				if err != nil {
					readErr = fmt.Errorf("the deployments table in %s: %w", path, err)
					break
				}
				if !seen[c.JSON()] {
					seen[c.JSON()] = true
					recorded = append(recorded, c)
				}
			}
		}
	}
	names := func() string {
		var parts []string
		for _, c := range recorded {
			parts = append(parts, c.JSON())
		}
		return strings.Join(parts, ", ")
	}
	if flagClaim != "" {
		c, err := ParseClaimJSON(m, flagClaim)
		if err != nil {
			return c, "", err
		}
		if readErr == nil && len(recorded) > 0 && (len(recorded) != 1 || recorded[0].JSON() != c.JSON()) {
			return c, fmt.Sprintf("-claim %s differs from the claim recorded in the deployments table (%s); using %s",
				c.JSON(), names(), c.JSON()), nil
		}
		return c, "", nil
	}
	if readErr != nil {
		return engine.Claim{}, "", fmt.Errorf("cannot read the claim from the deployments table: %w", readErr)
	}
	switch len(recorded) {
	case 0:
		return engine.ClaimWith(m, m.Declared()), "", nil
	case 1:
		return recorded[0], "", nil
	}
	return engine.Claim{}, "", fmt.Errorf("the deployments table in %s records several claims (%s); pass -claim", path, names())
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
