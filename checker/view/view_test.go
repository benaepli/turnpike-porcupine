package view

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

type fixture struct {
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Model       string            `json:"model"`
	Claim       map[string]string `json:"claim"`
	Events      []Event           `json:"events"`
	Expect      struct {
		Verdict string  `json:"verdict"`
		Reason  string  `json:"reason"`
		Triage  string  `json:"triage"`
		Witness *string `json:"witness"`
	} `json:"expect"`
}

func loadFixtures(t *testing.T) []fixture {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join("..", "testdata", "claims", "*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no claim fixtures found: %v", err)
	}
	var out []fixture
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var f fixture
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		out = append(out, f)
	}
	return out
}

func fixtureInput(t *testing.T, f fixture) Input {
	t.Helper()
	var w *Witness
	if f.Expect.Witness != nil {
		var err error
		if w, err = ParseWitness(*f.Expect.Witness); err != nil {
			t.Fatalf("%s: %v", f.Name, err)
		}
	}
	return Input{
		Title:   f.Name,
		Model:   f.Model,
		Claim:   f.Claim,
		Verdict: f.Expect.Verdict,
		Reason:  f.Expect.Reason,
		Triage:  f.Expect.Triage,
		Events:  f.Events,
		Witness: w,
	}
}

func render(t *testing.T, in Input) string {
	t.Helper()
	var buf bytes.Buffer
	if err := Render(&buf, in); err != nil {
		t.Fatalf("%s: render: %v", in.Title, err)
	}
	return buf.String()
}

func once(t *testing.T, name, page, needle string) {
	t.Helper()
	if n := strings.Count(page, needle); n != 1 {
		t.Errorf("%s: %s occurs %d times, want 1", name, needle, n)
	}
}

var urlRef = regexp.MustCompile(`url\(([^)]*)\)`)

var opRef = regexp.MustCompile(`data-op="-?[0-9]+"`)

// noExternal fails on anything that would make the page fetch or link
// outside itself.
func noExternal(t *testing.T, name, page string) {
	t.Helper()
	lower := strings.ToLower(page)
	for _, bad := range []string{"http://", "https://", "//cdn", " src=", " href=", "<link", "@import", "<iframe", "<img"} {
		if strings.Contains(lower, bad) {
			t.Errorf("%s: page contains %q", name, bad)
		}
	}
	for _, m := range urlRef.FindAllStringSubmatch(page, -1) {
		if !strings.HasPrefix(m[1], "#") {
			t.Errorf("%s: page references %s", name, m[0])
		}
	}
}

func TestFixturePages(t *testing.T) {
	outDir := os.Getenv("VIEW_OUT_DIR")
	if outDir != "" {
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	kinds := map[string]bool{}
	for _, f := range loadFixtures(t) {
		in := fixtureInput(t, f)
		page := render(t, in)
		noExternal(t, f.Name, page)

		ids := map[int64]bool{}
		for _, e := range f.Events {
			if e.Kind == "Invocation" && kindOf(e.Action) != "" && !ids[e.ID] {
				ids[e.ID] = true
				once(t, f.Name, page, fmt.Sprintf(`data-op="%d"`, e.ID))
			}
		}
		for _, e := range f.Events {
			if e.Kind == "Invocation" && kindOf(e.Action) == "" && !ids[e.ID] && strings.Contains(page, fmt.Sprintf(`data-op="%d"`, e.ID)) {
				t.Errorf("%s: system row %d (%s) drawn as an operation", f.Name, e.ID, e.Action)
			}
		}
		if n, want := len(opRef.FindAllString(page, -1)), len(ids); n != want {
			t.Errorf("%s: %d operations drawn, want %d", f.Name, n, want)
		}
		if !strings.Contains(page, `class="v verdict-`+f.Expect.Verdict+`"`) {
			t.Errorf("%s: verdict %q not shown", f.Name, f.Expect.Verdict)
		}
		if !strings.Contains(page, `<span class="v">`+f.Expect.Triage+`</span>`) {
			t.Errorf("%s: triage %q not shown", f.Name, f.Expect.Triage)
		}
		if w := in.Witness; w != nil {
			kinds[w.Kind] = true
			for _, e := range w.Cycle {
				once(t, f.Name, page, fmt.Sprintf(`data-edge="%d-%d"`, e.From, e.To))
			}
			for _, b := range w.Blocked {
				once(t, f.Name, page, fmt.Sprintf(`data-blocked="%d"`, b.Op))
				for _, a := range b.After {
					once(t, f.Name, page, fmt.Sprintf(`data-edge="%d-%d"`, a.Op, b.Op))
				}
			}
			edges := len(w.Cycle)
			for _, b := range w.Blocked {
				edges += len(b.After)
			}
			if n := strings.Count(page, `data-edge="`); n != edges {
				t.Errorf("%s: %d edges drawn, want %d", f.Name, n, edges)
			}
			if n := strings.Count(page, `data-blocked="`); n != len(w.Blocked) {
				t.Errorf("%s: %d blocked heads drawn, want %d", f.Name, n, len(w.Blocked))
			}
			for i, id := range w.Order {
				if !strings.Contains(page, fmt.Sprintf(`<li data-ref="%d"><span class="pos">%d</span>`, id, i+1)) {
					t.Errorf("%s: order position %d of op %d not listed", f.Name, i+1, id)
				}
			}
			if v := w.Value; v != nil {
				once(t, f.Name, page, fmt.Sprintf(`data-value-op="%d"`, v.Op))
				if v.Other != nil {
					once(t, f.Name, page, fmt.Sprintf(`data-value-other="%d"`, *v.Other))
				}
			}
		} else if strings.Contains(page, `data-edge="`) || strings.Contains(page, `data-blocked="`) {
			t.Errorf("%s: no witness, but edges or blocked heads drawn", f.Name)
		}
		if strings.Contains(page, "history does not hold") {
			t.Errorf("%s: witness names an operation missing from the history", f.Name)
		}
		if outDir != "" {
			if err := os.WriteFile(filepath.Join(outDir, f.Name+".html"), []byte(page), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, k := range []string{"order", "cycle", "frontier", "value"} {
		if !kinds[k] {
			t.Errorf("no fixture exercises witness kind %q", k)
		}
	}
}

func TestEmbeddedDataRoundTrips(t *testing.T) {
	for _, f := range loadFixtures(t) {
		page := render(t, fixtureInput(t, f))
		start := strings.Index(page, `<script type="application/json" id="run-data">`)
		if start < 0 {
			t.Fatalf("%s: no data block", f.Name)
		}
		rest := page[start+len(`<script type="application/json" id="run-data">`):]
		block := rest[:strings.Index(rest, "</script>")]
		var d dataBlock
		if err := json.Unmarshal([]byte(block), &d); err != nil {
			t.Fatalf("%s: data block: %v", f.Name, err)
		}
		if len(d.Events) != len(f.Events) {
			t.Errorf("%s: data block has %d events, want %d", f.Name, len(d.Events), len(f.Events))
		}
	}
}

func TestKeyIsEscaped(t *testing.T) {
	for _, f := range loadFixtures(t) {
		if f.Name != "lin_key_escaping" {
			continue
		}
		page := render(t, fixtureInput(t, f))
		if strings.Contains(page, "<&>") || strings.Contains(page, "\x01") {
			t.Errorf("key written unescaped")
		}
		return
	}
	t.Fatal("fixture lin_key_escaping not found")
}

func strp(s string) *string { return &s }
func intp(v int64) *int64   { return &v }

// A history with a clock, node labels, an overlapping session and a pending
// write exercises what the fixtures do not.
func TestGlobalTimeAndNodes(t *testing.T) {
	in := Input{
		Title: "synthetic_clock_and_faults",
		Model: "kv",
		Claim: map[string]string{"Read": "sequential", "Write": "linearizable"},
		Events: []Event{
			{Kind: "Invocation", ID: 1, Client: 3, Action: "Client.Write", Key: strp("x"), UID: intp(11), Step: 1, GlobalTime: 0},
			{Kind: "Crash", Node: 2, Step: 2, GlobalTime: 40},
			{Kind: "Invocation", ID: 2, Client: 3, Action: "Client.Read", Key: strp("x"), Step: 3, GlobalTime: 90},
			{Kind: "Response", ID: 1, Client: 3, Action: "Client.Write", Step: 4, GlobalTime: 100},
			{Kind: "Recover", Node: 2, Step: 5, GlobalTime: 160},
			{Kind: "Response", ID: 2, Client: 3, Action: "Client.Read", Value: []int64{11}, Step: 6, GlobalTime: 170},
			{Kind: "Invocation", ID: 3, Client: 4, Action: "Client.Write", Key: strp("x"), UID: intp(12), Step: 7, GlobalTime: 200},
			{Kind: "Crash", Node: 0, Step: 8, GlobalTime: 210},
		},
		Verdict: "ok",
		Triage:  "linearizable",
		Witness: &Witness{Level: "ordered_sequential", Kind: "order", Order: []int64{1, 2, 3}},
		NodeLabel: func(n int64) (string, bool) {
			if n == 2 {
				return "Node[2] at nodes[2]", true
			}
			return "", false
		},
	}
	page := render(t, in)
	noExternal(t, in.Title, page)
	if dir := os.Getenv("VIEW_OUT_DIR"); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "synthetic_clock_and_faults.html"), []byte(page), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, want := range []string{`data-axis-button="gt"`, `data-g-x="`, `data-g-transform="`, "Node[2] at nodes[2]", "Node 0", `class="downtime"`, "Node[2] at nodes[2] crashed at step 2", "ordered_sequential", "session 3 has operations that overlap"} {
		if !strings.Contains(page, want) {
			t.Errorf("page lacks %q", want)
		}
	}
	for id := 1; id <= 3; id++ {
		once(t, in.Title, page, fmt.Sprintf(`data-op="%d"`, id))
	}
	if n := strings.Count(page, `class="downtime"`); n != 2 {
		t.Errorf("%d downtime bands, want 2 (one closed, one open to the end)", n)
	}
}

func TestClaimName(t *testing.T) {
	cases := []struct {
		model string
		claim map[string]string
		want  string
	}{
		{"kv", map[string]string{"Read": "linearizable", "Write": "linearizable"}, "linearizable"},
		{"kv", map[string]string{"Read": "sequential", "Write": "sequential"}, "sequential"},
		{"kv", map[string]string{"Read": "sequential", "Write": "linearizable"}, "ordered_sequential"},
		{"kv", map[string]string{"Read": "linearizable", "Write": "sequential"}, "mixed"},
		{"kv_rmw", map[string]string{"RMW": "linearizable", "Read": "sequential", "Write": "linearizable"}, "ordered_sequential"},
		{"kv_rmw", map[string]string{"RMW": "sequential", "Read": "sequential", "Write": "linearizable"}, "mixed"},
	}
	for _, c := range cases {
		if got := claimName(c.model, c.claim); got != c.want {
			t.Errorf("claimName(%s, %v) = %s, want %s", c.model, c.claim, got, c.want)
		}
	}
}
