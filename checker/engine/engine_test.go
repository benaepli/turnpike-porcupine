package engine

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fixtureEvent struct {
	Kind   string   `json:"kind"`
	ID     int64    `json:"id"`
	Client int64    `json:"client"`
	Action string   `json:"action"`
	Key    *string  `json:"key"`
	UID    *int64   `json:"uid"`
	Value  *[]int64 `json:"value"`
}

type fixture struct {
	Name      string            `json:"name"`
	Model     string            `json:"model"`
	Claim     map[string]string `json:"claim"`
	Interrupt *string           `json:"interrupt"`
	Events    []fixtureEvent    `json:"events"`
	Expect    struct {
		Verdict string  `json:"verdict"`
		Reason  string  `json:"reason"`
		Triage  string  `json:"triage"`
		Witness *string `json:"witness"`
	} `json:"expect"`
}

const fixtureDir = "../testdata/claims"

func loadFixtures(t testing.TB) []fixture {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(fixtureDir, "*.json"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no fixtures in %s: %v", fixtureDir, err)
	}
	var out []fixture
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var f fixture
		if err := json.Unmarshal(data, &f); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		if f.Name+".json" != filepath.Base(p) {
			t.Fatalf("%s: name %q does not match the file", p, f.Name)
		}
		out = append(out, f)
	}
	return out
}

func (f *fixture) rows() []Row {
	rows := make([]Row, len(f.Events))
	for i, e := range f.Events {
		r := Row{ID: e.ID, Client: e.Client, Action: e.Action}
		switch e.Kind {
		case "Invocation":
			r.Kind = Invocation
		case "Response":
			r.Kind = Response
		default:
			r.Kind = System
		}
		if e.Key != nil {
			r.Key, r.HasKey = *e.Key, true
		}
		if e.UID != nil {
			r.UID, r.HasUID = *e.UID, true
		}
		if e.Value != nil {
			r.Value, r.HasValue = *e.Value, true
		}
		rows[i] = r
	}
	return rows
}

func (f *fixture) claim(t testing.TB) Claim {
	m, err := ParseModel(f.Model)
	if err != nil {
		t.Fatal(err)
	}
	c, err := ParseClaim(m, f.Claim)
	if err != nil {
		t.Fatalf("%s: %v", f.Name, err)
	}
	return c
}

func (f *fixture) interrupt() Interrupt {
	var intr Interrupt
	if f.Interrupt == nil {
		return intr
	}
	switch *f.Interrupt {
	case "deadline":
		intr.Deadline = time.Now().Add(-time.Second)
	case "cancel":
		intr.Cancel = new(atomic.Bool)
		intr.Cancel.Store(true)
	}
	return intr
}

func reasonMatches(got, want string) bool {
	return got == want || strings.HasPrefix(got, want+": ")
}

func TestFixtures(t *testing.T) {
	for _, f := range loadFixtures(t) {
		t.Run(f.Name, func(t *testing.T) {
			r := Check(f.rows(), f.claim(t), f.interrupt())
			if r.Verdict != f.Expect.Verdict {
				t.Errorf("verdict %q, want %q (reason %q)", r.Verdict, f.Expect.Verdict, r.Reason)
			}
			if !reasonMatches(r.Reason, f.Expect.Reason) {
				t.Errorf("reason %q, want %q", r.Reason, f.Expect.Reason)
			}
			if r.Triage != f.Expect.Triage {
				t.Errorf("triage %q, want %q", r.Triage, f.Expect.Triage)
			}
			switch {
			case f.Expect.Witness == nil && r.Witness != nil:
				t.Errorf("witness %s, want null", r.Witness)
			case f.Expect.Witness != nil && r.Witness == nil:
				t.Errorf("witness null, want %s", *f.Expect.Witness)
			case f.Expect.Witness != nil && r.Witness.String() != *f.Expect.Witness:
				t.Errorf("witness\n got %s\nwant %s", r.Witness, *f.Expect.Witness)
			}
		})
	}
}

func TestCanonicalStringEscapes(t *testing.T) {
	got := string(appendString(nil, "a\"\\\b\f\n\r\t\x01\x1f\x7f<>&\xe2\x80\xa8\xe2\x80\xa9\xc3\xa9"))
	want := "\"a\\\"\\\\\\b\\f\\n\\r\\t\\u0001\\u001f\x7f<>&\xe2\x80\xa8\xe2\x80\xa9\xc3\xa9\""
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestRowFromEvent(t *testing.T) {
	inv := RowFromEvent("7", "3", "Invocation", "raft::Client.Write",
		`["{\"type\":\"VUnit\",\"value\":null}","{\"type\":\"VString\",\"value\":\"k\"}","{\"type\":\"VInt\",\"value\":42}"]`)
	if inv.Kind != Invocation || inv.ID != 7 || inv.Client != 3 || !inv.HasKey || inv.Key != "k" || !inv.HasUID || inv.UID != 42 || inv.Malformed != "" {
		t.Fatalf("write invocation: %+v", inv)
	}
	resp := RowFromEvent("8", "3", "Response", "Client.Read",
		`[{"type":"VList","value":[{"type":"VInt","value":1},{"type":"VInt","value":2}]}]`)
	if !resp.HasValue || len(resp.Value) != 2 || resp.Value[1] != 2 || resp.Malformed != "" {
		t.Fatalf("read response: %+v", resp)
	}
	bad := RowFromEvent("9", "3", "Invocation", "Client.Write",
		`["null","{\"type\":\"VInt\",\"value\":1}","{\"type\":\"VInt\",\"value\":1}"]`)
	if bad.Malformed == "" {
		t.Fatalf("an integer key must be malformed: %+v", bad)
	}
	sys := RowFromEvent("-1", "-1", "Crash", "System.Crash", `[]`)
	if sys.Kind != System {
		t.Fatalf("crash row: %+v", sys)
	}
	for _, action := range []string{"Client.SimulateTimeout", "Client.Delete", "System.Crash"} {
		other := RowFromEvent("x", "y", "Invocation", action, `not json`)
		if other.Kind != System || other.Malformed != "" {
			t.Fatalf("%s invocation must be a system row: %+v", action, other)
		}
	}
	timer := RowFromEvent("4", "1", "TimerFired", "Client.Write", `[]`)
	if timer.Kind != System {
		t.Fatalf("a TimerFired row is a system row whatever its action: %+v", timer)
	}
	extraItems := []struct{ kind, action, payload string }{
		{"Invocation", "Client.Read", `[{"type":"VUnit","value":null},{"type":"VString","value":"k"},{"type":"VInt","value":1}]`},
		{"Invocation", "Client.Write", `[{"type":"VUnit","value":null},{"type":"VString","value":"k"}]`},
		{"Response", "Client.Read", `[{"type":"VList","value":[]},{"type":"VUnit","value":null}]`},
		{"Response", "Client.Read", `["{\"type\":\"VList\",\"value\":[]}","{\"type\":\"VUnit\",\"value\":null}"]`},
		{"Response", "Client.RMW", `[]`},
	}
	for _, c := range extraItems {
		if r := RowFromEvent("1", "1", c.kind, c.action, c.payload); r.Malformed == "" {
			t.Fatalf("%s %s %s must be malformed: %+v", c.kind, c.action, c.payload, r)
		}
	}
	if r := RowFromEvent("1", "1", "Response", "Client.Write", `[{"type":"VUnit","value":null},{"type":"VUnit","value":null}]`); r.Malformed != "" {
		t.Fatalf("a write response's payload is not read: %+v", r)
	}
	none := RowFromEvent("1", "1", "Response", "Client.Read", `[{"type":"VOption","value":null}]`)
	if none.Malformed == "" || none.HasValue {
		t.Fatalf("an empty option is no value: %+v", none)
	}
	some := RowFromEvent("1", "1", "Response", "Client.Read",
		`[{"type":"VOption","value":{"type":"VList","value":[{"type":"VInt","value":5}]}}]`)
	if some.Malformed != "" || !some.HasValue || len(some.Value) != 1 || some.Value[0] != 5 {
		t.Fatalf("an option holding a list is read through: %+v", some)
	}
}
