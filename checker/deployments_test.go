package checker

import (
	"database/sql"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// writeParquet materializes a SQL query into dir/table/part-0.parquet.
func writeParquet(t *testing.T, db *sql.DB, dir, table, query string) {
	t.Helper()
	tableDir := filepath.Join(dir, table)
	if err := os.MkdirAll(tableDir, 0o755); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(tableDir, "part-0.parquet")
	if _, err := db.Exec("COPY (" + query + ") TO '" + out + "' (FORMAT parquet)"); err != nil {
		t.Fatalf("write %s: %v", table, err)
	}
}

// parquetOutput builds an output directory with an executions table and, for
// each non-empty argument, the named table.
func parquetOutput(t *testing.T, deployments, nodes, runs string) string {
	t.Helper()
	dir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	writeParquet(t, db, dir, "executions", `SELECT 0::BIGINT AS run_id, 0::BIGINT AS seq_num, 1::INTEGER AS unique_id,
		0::INTEGER AS client_id, 'Invocation' AS kind, 'Client.Read' AS action, '[]' AS payload`)
	if deployments != "" {
		writeParquet(t, db, dir, "deployments", deployments)
	}
	if nodes != "" {
		writeParquet(t, db, dir, "deployment_nodes", nodes)
	}
	if runs != "" {
		writeParquet(t, db, dir, "runs", runs)
	}
	return dir
}

func deploymentsQuery(models ...string) string {
	parts := make([]string, len(models))
	for i, m := range models {
		parts[i] = "SELECT " + strconv.Itoa(i) + "::INTEGER AS deployment_id, 'Single' AS deploy, 'KVClient' AS client, '" + m +
			"' AS model, '{}' AS params, '[]' AS aliases, 7::UBIGINT AS hash, 3::INTEGER AS node_count, " +
			`'{"Node":3}' AS roles, '[]' AS groups`
	}
	return strings.Join(parts, " UNION ALL ")
}

func TestResolveModelReadsTheDeploymentsTable(t *testing.T) {
	dir := parquetOutput(t, deploymentsQuery("kv_rmw", "kv_rmw"), "", "")
	model, warning, err := ResolveModel("", dir)
	if err != nil || model != "kv_rmw" || warning != "" {
		t.Fatalf("got model %q warning %q err %v, want kv_rmw with no warning", model, warning, err)
	}
	model, _, err = ResolveModel("", filepath.Join(dir, "executions"))
	if err != nil || model != "kv_rmw" {
		t.Fatalf("the executions directory itself: got %q err %v", model, err)
	}
}

func TestResolveModelFlagWinsWithWarning(t *testing.T) {
	dir := parquetOutput(t, deploymentsQuery("kv_rmw"), "", "")
	model, warning, err := ResolveModel("kv", dir)
	if err != nil || model != "kv" {
		t.Fatalf("got model %q err %v, want kv", model, err)
	}
	if !strings.Contains(warning, "kv_rmw") {
		t.Fatalf("a flag that differs from the table must warn, got %q", warning)
	}
	model, warning, err = ResolveModel("kv_rmw", dir)
	if err != nil || model != "kv_rmw" || warning != "" {
		t.Fatalf("an agreeing flag: got %q warning %q err %v", model, warning, err)
	}
}

func TestResolveModelErrors(t *testing.T) {
	several := parquetOutput(t, deploymentsQuery("kv", "kv_rmw"), "", "")
	if _, _, err := ResolveModel("", several); err == nil || !strings.Contains(err.Error(), "several") {
		t.Fatalf("several models must be an error, got %v", err)
	}
	if model, _, err := ResolveModel("kv", several); err != nil || model != "kv" {
		t.Fatalf("a flag resolves several models: got %q err %v", model, err)
	}
	missing := parquetOutput(t, "", "", "")
	if _, _, err := ResolveModel("", missing); err == nil {
		t.Fatal("a missing deployments table without -model must be an error")
	}
	if model, warning, err := ResolveModel("queue", missing); err != nil || model != "queue" || warning != "" {
		t.Fatalf("a flag without a deployments table: got %q warning %q err %v", model, warning, err)
	}
}

func TestReadTopologyNamesNodesPerRun(t *testing.T) {
	nodes := `SELECT * FROM (VALUES
		(0::INTEGER, 0::INTEGER, 'Node', 0::INTEGER, 'nodes[0]'),
		(0, 1, 'Node', 1, 'nodes[1]'),
		(1, 0, 'Router', 0, NULL),
		(1, 1, 'Node', 0, 'shards[0].nodes[0]')
	) AS t(deployment_id, node_index, role, ordinal, path)`
	runs := `SELECT * FROM (VALUES (0::BIGINT, 0::INTEGER, '{"n":2}'), (1, 1, '{"n":1}'), (2, -1, '{"n":9}'))
		AS t(run_id, deployment_id, params)`
	dir := parquetOutput(t, deploymentsQuery("kv"), nodes, runs)

	topo, err := ReadTopology(dir)
	if err != nil || topo == nil {
		t.Fatalf("ReadTopology: %v %v", topo, err)
	}
	names := topo.NamesForRun(1)
	if names == nil {
		t.Fatal("run 1 has a deployment")
	}
	if l, ok := names(0); !ok || l != (NodeLabel{Role: "Router", Ordinal: 0}) {
		t.Fatalf("run 1 node 0: %+v %v", l, ok)
	}
	if l, ok := names(1); !ok || l.Path != "shards[0].nodes[0]" {
		t.Fatalf("run 1 node 1: %+v %v", l, ok)
	}
	if _, ok := names(5); ok {
		t.Fatal("a client node has no label")
	}
	if topo.NamesForRun(2) != nil {
		t.Fatal("a run without a deployment has no names")
	}

	if topo, err := ReadTopology(parquetOutput(t, "", "", "")); err != nil || topo != nil {
		t.Fatalf("without deployment_nodes: %v %v", topo, err)
	}
	if (*Topology)(nil).NamesForRun(0) != nil {
		t.Fatal("a nil topology names nothing")
	}
}
