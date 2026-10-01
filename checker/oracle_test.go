package checker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/benaepli/turnpike-porcupine/checker/engine"
)

// actionText restores the action suffix the engine reads from a parsed
// action type.
func actionText(a ActionType) string {
	switch a {
	case Read:
		return "Client.Read"
	case Write:
		return "Client.Write"
	case Rmw:
		return "Client.RMW"
	case Delete:
		return "Client.Delete"
	case Timeout:
		return "Client.SimulateTimeout"
	case Crash:
		return "System.Crash"
	case Recover:
		return "System.Recover"
	}
	return string(a)
}

func engineRows(events []*EventRow) []engine.Row {
	rows := make([]engine.Row, len(events))
	for i, e := range events {
		rows[i] = engine.RowFromEvent(e.UniqueID, e.ClientID, e.Kind, actionText(e.Action), e.Payload)
	}
	return rows
}

func upstreamModel(m engine.Model) porcupine.Model {
	if m == engine.KVRMW {
		return KVRMWModel()
	}
	return KVModel()
}

type oracleFixture struct {
	Name   string            `json:"name"`
	Model  string            `json:"model"`
	Claim  map[string]string `json:"claim"`
	Events []struct {
		Kind   string   `json:"kind"`
		ID     int64    `json:"id"`
		Client int64    `json:"client"`
		Action string   `json:"action"`
		Key    *string  `json:"key"`
		UID    *int64   `json:"uid"`
		Value  *[]int64 `json:"value"`
	} `json:"events"`
	Expect struct {
		Verdict string `json:"verdict"`
	} `json:"expect"`
}

// TestOracleFixtures checks every fixture whose claim is linearizable with
// upstream porcupine as well as the engine.
func TestOracleFixtures(t *testing.T) {
	paths, _ := filepath.Glob("testdata/claims/*.json")
	checked := 0
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		var f oracleFixture
		if err := json.Unmarshal(data, &f); err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		m, err := engine.ParseModel(f.Model)
		if err != nil {
			t.Fatal(err)
		}
		c, err := engine.ParseClaim(m, f.Claim)
		if err != nil {
			t.Fatal(err)
		}
		if c.Name() != "linearizable" || f.Expect.Verdict == engine.VerdictUnknown {
			continue
		}
		var rows []engine.Row
		var ops []porcupine.Operation
		calls := map[int64]int{}
		tm := int64(0)
		for _, e := range f.Events {
			r := engine.Row{ID: e.ID, Client: e.Client, Action: e.Action}
			switch e.Kind {
			case "Invocation":
				r.Kind = engine.Invocation
			case "Response":
				r.Kind = engine.Response
			default:
				r.Kind = engine.System
				rows = append(rows, r)
				continue
			}
			tm++
			k, _ := engine.ActionKind(e.Action)
			if e.Key != nil {
				r.Key, r.HasKey = *e.Key, true
			}
			if e.UID != nil {
				r.UID, r.HasUID = *e.UID, true
			}
			if e.Value != nil {
				r.Value, r.HasValue = *e.Value, true
			}
			rows = append(rows, r)
			if r.Kind == engine.Invocation {
				op := map[engine.Kind]string{engine.Write: "PUT", engine.Read: "GET", engine.RMW: "RMW"}[k]
				calls[r.ID] = len(ops)
				ops = append(ops, porcupine.Operation{ClientId: int(r.Client), Call: tm, Return: -1,
					Input: KVInput{Op: op, Key: "\"" + r.Key + "\"", Uid: int(r.UID)}})
				continue
			}
			o := &ops[calls[r.ID]]
			o.Return = tm
			if r.HasValue {
				uids := make([]int, len(r.Value))
				for i, u := range r.Value {
					uids[i] = int(u)
				}
				o.Output = makeUidListValue(uids)
			}
		}
		var kept []porcupine.Operation
		for _, o := range ops {
			if o.Return < 0 {
				if o.Input.(KVInput).Op == "GET" {
					continue
				}
				o.Return = tm + 1
			}
			kept = append(kept, o)
		}
		want := porcupine.CheckOperations(upstreamModel(m), kept)
		got := engine.Check(rows, c, engine.Interrupt{})
		if (got.Verdict == engine.VerdictOK) != want {
			t.Errorf("%s: engine %s, upstream linearizable=%v", f.Name, got.Verdict, want)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no linearizable fixture checked")
	}
	t.Logf("%d fixtures agree with upstream porcupine", checked)
}

type corpusRun struct {
	id     int
	rows   []engine.Row
	events []*EventRow
}

var corpusOnce struct {
	sync.Once
	model engine.Model
	runs  []corpusRun
	err   error
}

// loadCorpus reads every run of the output directory SPUR_ORACLE_CORPUS
// names; the test is skipped when it is unset.
func loadCorpus(t testing.TB) (engine.Model, []corpusRun) {
	path := os.Getenv("SPUR_ORACLE_CORPUS")
	if path == "" {
		t.Skip("SPUR_ORACLE_CORPUS is not set")
	}
	corpusOnce.Do(func() {
		name, _, err := ResolveModel("", path)
		if err != nil {
			corpusOnce.err = err
			return
		}
		if corpusOnce.model, err = engine.ParseModel(name); err != nil {
			corpusOnce.err = err
			return
		}
		corpusOnce.err = ProcessAllRunsFromDuckDB(path, func(id int, events []*EventRow) error {
			corpusOnce.runs = append(corpusOnce.runs, corpusRun{id, engineRows(events), events})
			return nil
		})
	})
	if corpusOnce.err != nil {
		t.Fatal(corpusOnce.err)
	}
	return corpusOnce.model, corpusOnce.runs
}

// TestOracleCorpus checks every history of a corpus with the engine's
// linearizable claim and with upstream porcupine, and fails on any flip.
func TestOracleCorpus(t *testing.T) {
	m, runs := loadCorpus(t)
	c := engine.ClaimWith(m, m.Declared())
	model := upstreamModel(m)
	counts := map[string]int{}
	for _, r := range runs {
		got := engine.Check(r.rows, c, engine.Interrupt{Deadline: time.Now().Add(10 * time.Second)})
		if got.Verdict == engine.VerdictUnknown {
			counts["engine unknown: "+got.Reason]++
			continue
		}
		res := porcupine.CheckOperationsTimeout(model, BuildOperations(r.events), 10*time.Second)
		switch {
		case res == porcupine.Unknown:
			counts["upstream unknown"]++
		case (res == porcupine.Ok) != (got.Verdict == engine.VerdictOK):
			t.Errorf("run %d: engine %s, upstream %v", r.id, got.Verdict, res)
			counts["flip"]++
		default:
			counts["agree "+got.Verdict+" triage "+got.Triage]++
		}
	}
	t.Logf("%d runs: %v", len(runs), counts)
}

// BenchmarkCorpusEngine checks every history of the corpus with the engine:
// the linearizable claim alone, as the stage every history runs.
func BenchmarkCorpusEngine(b *testing.B) {
	m, runs := loadCorpus(b)
	c := engine.ClaimWith(m, m.Declared())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, r := range runs {
			engine.Prepare(r.rows, m).CheckClaim(c, engine.Interrupt{})
		}
	}
}

// BenchmarkCorpusEngineWithTriage adds the ladder walk and the witness.
func BenchmarkCorpusEngineWithTriage(b *testing.B) {
	m, runs := loadCorpus(b)
	c := engine.ClaimWith(m, m.Declared())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, r := range runs {
			engine.Check(r.rows, c, engine.Interrupt{})
		}
	}
}

// BenchmarkCorpusEngineFromEvents includes converting the reader's rows.
func BenchmarkCorpusEngineFromEvents(b *testing.B) {
	m, runs := loadCorpus(b)
	c := engine.ClaimWith(m, m.Declared())
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, r := range runs {
			engine.Prepare(engineRows(r.events), m).CheckClaim(c, engine.Interrupt{})
		}
	}
}

// BenchmarkCorpusUpstream checks every history with upstream porcupine,
// building its operations from the reader's rows.
func BenchmarkCorpusUpstream(b *testing.B) {
	m, runs := loadCorpus(b)
	model := upstreamModel(m)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, r := range runs {
			porcupine.CheckOperations(model, BuildOperations(r.events))
		}
	}
}
