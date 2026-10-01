package checker

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benaepli/turnpike-porcupine/checker/engine"
	"github.com/benaepli/turnpike-porcupine/checker/view"
)

type claimFixture struct {
	Name      string            `json:"name"`
	Model     string            `json:"model"`
	Claim     map[string]string `json:"claim"`
	Interrupt *string           `json:"interrupt"`
	Events    []struct {
		Kind       string   `json:"kind"`
		ID         int64    `json:"id"`
		Client     int64    `json:"client"`
		Action     string   `json:"action"`
		Key        *string  `json:"key"`
		UID        *int64   `json:"uid"`
		Value      *[]int64 `json:"value"`
		Node       int64    `json:"node"`
		Step       int64    `json:"step"`
		GlobalTime int64    `json:"global_time"`
	} `json:"events"`
	Expect struct {
		Verdict string  `json:"verdict"`
		Reason  string  `json:"reason"`
		Triage  string  `json:"triage"`
		Witness *string `json:"witness"`
	} `json:"expect"`
}

func loadClaimFixtures(t *testing.T) []claimFixture {
	t.Helper()
	paths, err := filepath.Glob("testdata/claims/*.json")
	if err != nil || len(paths) == 0 {
		t.Fatalf("no claim fixtures: %v", err)
	}
	var out []claimFixture
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var f claimFixture
		if err := json.Unmarshal(data, &f); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		out = append(out, f)
	}
	return out
}

func valueItem(typ string, v interface{}) string {
	raw, _ := json.Marshal(v)
	item, _ := json.Marshal(map[string]json.RawMessage{"type": json.RawMessage(fmt.Sprintf("%q", typ)), "value": raw})
	quoted, _ := json.Marshal(string(item))
	return string(quoted)
}

func listItem(xs []int64) string {
	items := make([]json.RawMessage, len(xs))
	for i, x := range xs {
		items[i] = json.RawMessage(fmt.Sprintf(`{"type":"VInt","value":%d}`, x))
	}
	return valueItem("VList", items)
}

// eventRows renders a fixture as executions-table rows, payloads in the
// simulator's shape, so the fixture runs through the same conversion a
// corpus does.
func eventRows(f claimFixture) []*EventRow {
	var rows []*EventRow
	for _, e := range f.Events {
		r := &EventRow{UniqueID: fmt.Sprint(e.ID), ClientID: fmt.Sprint(e.Client), Kind: e.Kind, Action: e.Action,
			Step: e.Step, GlobalTime: e.GlobalTime}
		var items []string
		switch e.Kind {
		case "Invocation":
			items = append(items, valueItem("VUnit", nil))
			if e.Key != nil {
				items = append(items, valueItem("VString", *e.Key))
			}
			if e.UID != nil {
				items = append(items, valueItem("VInt", *e.UID))
			}
		case "Response":
			if e.Value != nil {
				items = append(items, listItem(*e.Value))
			} else if k, ok := engine.ActionKind(e.Action); ok && k == engine.Write {
				items = append(items, valueItem("VUnit", nil))
			}
		default:
			r.UniqueID, r.ClientID = "-1", "-1"
			items = append(items, valueItem("VNode", map[string]int64{"role": 0, "index": e.Node}))
		}
		r.Payload = "[" + strings.Join(items, ",") + "]"
		rows = append(rows, r)
	}
	return rows
}

// TestStagedCheckMatchesFixtures runs every fixture without an interrupt
// through the commands' conversion and staged check: verdict, reason,
// triage and witness bytes as the contract expects, with or without the
// witness of a linearizable history.
func TestStagedCheckMatchesFixtures(t *testing.T) {
	checked := 0
	for _, f := range loadClaimFixtures(t) {
		if f.Interrupt != nil {
			continue
		}
		m, err := engine.ParseModel(f.Model)
		if err != nil {
			t.Fatal(err)
		}
		c, err := engine.ParseClaim(m, f.Claim)
		if err != nil {
			t.Fatal(err)
		}
		events := eventRows(f)
		rows := EngineRows(events)
		for _, witness := range []bool{true, false} {
			o := CheckHistory(rows, c, 0, witness)
			if o.Verdict != f.Expect.Verdict || o.Triage != f.Expect.Triage ||
				!(o.Reason == f.Expect.Reason || strings.HasPrefix(o.Reason, f.Expect.Reason+": ")) {
				t.Errorf("%s (witness %v): got %s %q %s, want %s %q %s", f.Name, witness,
					o.Verdict, o.Reason, o.Triage, f.Expect.Verdict, f.Expect.Reason, f.Expect.Triage)
			}
			want := ""
			if f.Expect.Witness != nil {
				want = *f.Expect.Witness
			}
			if (witness || o.Verdict == engine.VerdictIllegal) && o.Witness != want {
				t.Errorf("%s (witness %v): witness\n got %s\nwant %s", f.Name, witness, o.Witness, want)
			}
		}

		ve := ViewEvents(events, rows)
		o := CheckHistory(rows, c, 0, true)
		in, err := ViewInput(f.Name, c, o, ve, nil)
		if err != nil {
			t.Fatalf("%s: %v", f.Name, err)
		}
		var page strings.Builder
		if err := view.Render(&page, in); err != nil {
			t.Fatalf("%s: render: %v", f.Name, err)
		}
		if o.Verdict == engine.VerdictIllegal {
			text, err := ClaimText(f.Name, c, o, ve)
			if err != nil || !strings.Contains(text, "triage: "+o.Triage) {
				t.Errorf("%s: claim text %q, %v", f.Name, text, err)
			}
			w, _ := view.ParseWitness(o.Witness)
			if sig := WitnessSignature(f.Model, w, ve); len(sig) != 16 {
				t.Errorf("%s: signature %q", f.Name, sig)
			}
		}
		checked++
	}
	if checked < 30 {
		t.Fatalf("only %d fixtures checked", checked)
	}
}

func TestSignatureIgnoresIdsAndRotation(t *testing.T) {
	key := func(s string) *string { return &s }
	uid := func(n int64) *int64 { return &n }
	events := func(base int64) []view.Event {
		return []view.Event{
			{Kind: "Invocation", ID: base + 1, Action: "Client.Write", Key: key("x"), UID: uid(base + 10)},
			{Kind: "Invocation", ID: base + 2, Action: "Client.Read", Key: key("y")},
			{Kind: "Invocation", ID: base + 3, Action: "Client.Write", Key: key("y"), UID: uid(base + 11)},
			{Kind: "Invocation", ID: base + 4, Action: "Client.Read", Key: key("x")},
		}
	}
	cycle := func(base int64, rot int) *view.Witness {
		e := []view.Edge{{From: base + 1, To: base + 2, Relation: "session"}, {From: base + 2, To: base + 3, Relation: "rw"},
			{From: base + 3, To: base + 4, Relation: "session"}, {From: base + 4, To: base + 1, Relation: "rw"}}
		e = append(e[rot:], e[:rot]...)
		return &view.Witness{Level: "sequential", Kind: "cycle", Cycle: e, Order: []int64{base + 7}}
	}
	a := WitnessSignature("kv", cycle(0, 0), events(0))
	b := WitnessSignature("kv", cycle(100, 2), events(100))
	if a != b {
		t.Fatalf("same shape, different signatures: %s %s", a, b)
	}
	c := WitnessSignature("kv", &view.Witness{Level: "sequential", Kind: "cycle", Cycle: cycle(0, 0).Cycle[:2]}, events(0))
	if a == c {
		t.Fatal("different shapes share a signature")
	}
}

func TestSkippedOpsCountsUnknownActions(t *testing.T) {
	rows := []*EventRow{
		{Kind: "Invocation", Action: "Client.Write"},
		{Kind: "Invocation", Action: "Client.Delete"},
		{Kind: "Response", Action: "Client.Frobnicate"},
		{Kind: "Invocation", Action: "System.Crash"},
		{Kind: "TimerFired", Action: "Node.Tick"},
		{Kind: "Crash", Action: "System.Crash"},
	}
	if n := SkippedOps(rows); n != 1 {
		t.Fatalf("skipped %d, want 1", n)
	}
}

func deploymentsWithClaims(claims ...string) string {
	parts := make([]string, len(claims))
	for i, c := range claims {
		parts[i] = fmt.Sprintf(`SELECT %d::INTEGER AS deployment_id, 'kv_rmw' AS model, '%s' AS claim`, i, c)
	}
	return strings.Join(parts, " UNION ALL ")
}

func TestResolveClaim(t *testing.T) {
	ordered := `{"RMW":"linearizable","Read":"sequential","Write":"linearizable"}`
	dir := parquetOutput(t, deploymentsWithClaims(ordered, ordered), "", "")
	c, warning, err := ResolveClaim("", engine.KVRMW, dir)
	if err != nil || warning != "" || c.JSON() != ordered || c.Name() != "ordered_sequential" {
		t.Fatalf("got %s %q %v", c.JSON(), warning, err)
	}
	// Under kv the RMW mark is not declared and is dropped.
	c, _, err = ResolveClaim("", engine.KV, dir)
	if err != nil || c.JSON() != `{"Read":"sequential","Write":"linearizable"}` {
		t.Fatalf("kv: got %s %v", c.JSON(), err)
	}
	c, warning, err = ResolveClaim(`{"Read":"sequential","Write":"sequential"}`, engine.KVRMW, dir)
	if err != nil || !strings.Contains(warning, "differs") || c.JSON() != `{"RMW":"linearizable","Read":"sequential","Write":"sequential"}` {
		t.Fatalf("flag: got %s %q %v", c.JSON(), warning, err)
	}
	if _, warning, err = ResolveClaim(ordered, engine.KVRMW, dir); err != nil || warning != "" {
		t.Fatalf("an agreeing flag: %q %v", warning, err)
	}

	several := parquetOutput(t, deploymentsWithClaims(ordered, `{"RMW":"sequential","Read":"sequential","Write":"sequential"}`), "", "")
	if _, _, err := ResolveClaim("", engine.KVRMW, several); err == nil || !strings.Contains(err.Error(), "several") {
		t.Fatalf("several claims must be an error, got %v", err)
	}
	// One claim written two ways is one claim.
	spaced := parquetOutput(t, deploymentsWithClaims(ordered, `{"Write": "linearizable", "Read": "sequential", "RMW": "linearizable"}`), "", "")
	if c, _, err := ResolveClaim("", engine.KVRMW, spaced); err != nil || c.JSON() != ordered {
		t.Fatalf("spelling: got %s %v", c.JSON(), err)
	}

	for _, older := range []string{parquetOutput(t, deploymentsQuery("kv"), "", ""), parquetOutput(t, "", "", ""), ""} {
		c, warning, err := ResolveClaim("", engine.KV, older)
		if err != nil || warning != "" || c.Name() != "linearizable" {
			t.Fatalf("an older corpus means every kind linearizable: got %s %q %v", c.JSON(), warning, err)
		}
	}
	if _, _, err := ResolveClaim(`{"Read":"eventual"}`, engine.KV, ""); err == nil {
		t.Fatal("an unknown mark must be an error")
	}
}

func TestReaderCarriesStepAndGlobalTime(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	writeParquet(t, db, dir, "executions", `SELECT * FROM (VALUES
		(0::BIGINT, 0::BIGINT, 1::BIGINT, 0::BIGINT, 'Invocation', 'Client.Read', '[]', 3::INTEGER, 40::UBIGINT),
		(0, 1, -1, -1, 'Crash', 'System.Crash', '[]', 4, 50),
		(0, 2, -1, -1, 'TimerFired', 'Node.Tick', '[]', 5, 60),
		(0, 3, 1, 0, 'Response', 'Client.Read', '[]', 6, 70)
	) AS t(run_id, seq_num, unique_id, client_id, kind, action, payload, step, global_time)`)

	var got []*EventRow
	if err := ProcessAllRunsFromDuckDB(dir, func(id int, events []*EventRow) error {
		got = events
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[1].Kind != "Crash" || got[2].Step != 6 || got[2].GlobalTime != 70 || got[0].Action != "Client.Read" {
		t.Fatalf("bulk reader: %+v", got)
	}
	single, err := ReadEventsFromDuckDB(dir, 0)
	if err != nil || len(single) != 4 || single[2].Step != 5 || single[2].GlobalTime != 60 {
		t.Fatalf("single run reader: %+v %v", single, err)
	}

	older := parquetOutput(t, "", "", "")
	single, err = ReadEventsFromDuckDB(older, 0)
	if err != nil || len(single) != 1 || single[0].Step != 0 || single[0].GlobalTime != 0 {
		t.Fatalf("executions without time columns: %+v %v", single, err)
	}
}
