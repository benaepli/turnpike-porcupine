package engine

import (
	"math/rand"
	"slices"
	"testing"

	"github.com/anishathalye/porcupine"
)

type upstreamInput struct {
	kind Kind
	key  string
	uid  int64
}

// upstreamModel is the model as upstream porcupine states it: one step per
// operation over a map of lists, partitioned by key.
func upstreamModel(m Model) porcupine.Model {
	return porcupine.Model{
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			byKey := map[string][]porcupine.Operation{}
			var keys []string
			for _, o := range history {
				k := o.Input.(upstreamInput).key
				if _, ok := byKey[k]; !ok {
					keys = append(keys, k)
				}
				byKey[k] = append(byKey[k], o)
			}
			var out [][]porcupine.Operation
			for _, k := range keys {
				out = append(out, byKey[k])
			}
			return out
		},
		Init: func() interface{} { return []int64(nil) },
		Step: func(state, input, output interface{}) (bool, interface{}) {
			cur := state.([]int64)
			in := input.(upstreamInput)
			switch {
			case in.kind == Read:
				return slices.Equal(output.([]int64), cur), cur
			case in.kind == RMW:
				next := append(slices.Clone(cur), in.uid)
				if output == nil {
					return true, next
				}
				return slices.Equal(output.([]int64), cur), next
			case m == KVRMW:
				return true, []int64{in.uid}
			}
			return true, append(slices.Clone(cur), in.uid)
		},
		Equal: func(a, b interface{}) bool { return slices.Equal(a.([]int64), b.([]int64)) },
	}
}

// upstreamOperations numbers client rows as the contract does: pending
// writes return after every row, pending reads are dropped.
func upstreamOperations(rows []Row) []porcupine.Operation {
	var ops []porcupine.Operation
	at := map[int64]int{}
	t := int64(0)
	for _, r := range rows {
		if r.Kind == System {
			continue
		}
		t++
		k, _ := ActionKind(r.Action)
		if r.Kind == Invocation {
			at[r.ID] = len(ops)
			ops = append(ops, porcupine.Operation{ClientId: int(r.Client), Input: upstreamInput{k, r.Key, r.UID}, Call: t, Return: -1})
			continue
		}
		o := &ops[at[r.ID]]
		o.Return = t
		if k != Write {
			o.Output = r.Value
		}
	}
	var out []porcupine.Operation
	for _, o := range ops {
		if o.Return < 0 {
			if o.Input.(upstreamInput).kind == Read {
				continue
			}
			o.Return = t + 1
		}
		out = append(out, o)
	}
	return out
}

// TestUpstreamAgreement checks the linearizable claim against upstream
// porcupine on random histories of both models.
func TestUpstreamAgreement(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	iterations := 6000
	if testing.Short() {
		iterations = 600
	}
	agree := map[string]int{}
	for it := 0; it < iterations; it++ {
		m := Model(it % 2)
		rows := genHistory(r, m, 24, 5, 3)
		res := Check(rows, ClaimWith(m, m.Declared()), Interrupt{})
		want := porcupine.CheckOperations(upstreamModel(m), upstreamOperations(rows))
		if (res.Verdict == VerdictOK) != want || res.Verdict == VerdictUnknown {
			t.Fatalf("%s: engine %s (%s), upstream linearizable=%v\n%s", m, res.Verdict, res.Reason, want, describe(rows))
		}
		agree[m.String()+"/"+res.Verdict]++
	}
	t.Logf("agreements: %v", agree)
}
