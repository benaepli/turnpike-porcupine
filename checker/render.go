package checker

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"

	"github.com/benaepli/turnpike-porcupine/checker/engine"
	"github.com/benaepli/turnpike-porcupine/checker/view"
)

// ViewEvents converts history rows for the view: the client operation rows,
// with the fields the engine reads from their payloads, and the crash and
// recover rows with their node. Other rows are not drawn and are left out.
func ViewEvents(events []*EventRow, rows []engine.Row) []view.Event {
	out := make([]view.Event, 0, len(events))
	for i, e := range events {
		switch e.Kind {
		case "Crash", "Recover":
			out = append(out, view.Event{Kind: e.Kind, Node: int64(extractNodeID(e.Payload)), Step: e.Step, GlobalTime: e.GlobalTime})
		case "Invocation", "Response":
			r := rows[i]
			if r.Kind == engine.System {
				continue
			}
			ev := view.Event{Kind: e.Kind, ID: r.ID, Client: r.Client, Action: e.Action, Step: e.Step, GlobalTime: e.GlobalTime}
			if r.HasKey {
				key := r.Key
				ev.Key = &key
			}
			if r.HasUID {
				uid := r.UID
				ev.UID = &uid
			}
			if r.HasValue {
				ev.Value = append([]int64{}, r.Value...)
			}
			out = append(out, ev)
		}
	}
	return out
}

// ViewNodeLabel names nodes for the view's system lanes with the text the
// annotations use; nil names gives nil.
func ViewNodeLabel(names NodeNames) func(int64) (string, bool) {
	if names == nil {
		return nil
	}
	return func(node int64) (string, bool) {
		if _, ok := names(int(node)); !ok {
			return "", false
		}
		_, who := nodeText(names, int(node))
		return who, true
	}
}

// ClaimMarks is a claim's marks keyed by the operation names the view uses.
func ClaimMarks(c engine.Claim) map[string]string {
	marks := make(map[string]string)
	for _, k := range []engine.Kind{engine.Write, engine.Read, engine.RMW} {
		if c.Model.Declared().Has(k) {
			marks[k.String()] = c.Marks[k].String()
		}
	}
	return marks
}

// ViewInput assembles one history's page.
func ViewInput(title string, c engine.Claim, o Outcome, events []view.Event, names NodeNames) (view.Input, error) {
	w, err := view.ParseWitness(o.Witness)
	if err != nil {
		return view.Input{}, err
	}
	return view.Input{
		Title:     title,
		Model:     c.Model.String(),
		Claim:     ClaimMarks(c),
		Verdict:   o.Verdict,
		Reason:    o.Reason,
		Triage:    o.Triage,
		Events:    events,
		Witness:   w,
		NodeLabel: ViewNodeLabel(names),
	}, nil
}

type opInfo struct {
	kind  string
	key   string
	uid   int64
	value []int64
	has   bool
}

func operations(events []view.Event) map[int64]*opInfo {
	ops := make(map[int64]*opInfo)
	for _, e := range events {
		k, ok := engine.ActionKind(e.Action)
		if !ok {
			continue
		}
		switch e.Kind {
		case "Invocation":
			if _, dup := ops[e.ID]; dup {
				continue
			}
			o := &opInfo{kind: k.String()}
			if e.Key != nil {
				o.key = *e.Key
			}
			if e.UID != nil {
				o.uid = *e.UID
			}
			ops[e.ID] = o
		case "Response":
			if o, ok := ops[e.ID]; ok && e.Value != nil {
				o.value, o.has = e.Value, true
			}
		}
	}
	return ops
}

func ints(xs []int64) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = strconv.FormatInt(x, 10)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func describe(ops map[int64]*opInfo, id int64) string {
	o, ok := ops[id]
	if !ok {
		return fmt.Sprintf("op %d", id)
	}
	s := fmt.Sprintf("op %d %s %q", id, o.kind, o.key)
	if o.kind != engine.Read.String() {
		s += fmt.Sprintf(" uid %d", o.uid)
	}
	if o.has {
		s += " -> " + ints(o.value)
	}
	return s
}

// ClaimText renders a claim witness as text: the cycle with its labelled
// edges, the partial witness and each blocked head with its reason, or the
// value failure.
func ClaimText(label string, c engine.Claim, o Outcome, events []view.Event) (string, error) {
	w, err := view.ParseWitness(o.Witness)
	if err != nil {
		return "", err
	}
	ops := operations(events)
	var b strings.Builder
	fmt.Fprintf(&b, "%s: claim %s %s is %s\n", label, c.Name(), c.JSON(), o.Verdict)
	fmt.Fprintf(&b, "triage: %s\n", o.Triage)
	if o.Reason != "" {
		fmt.Fprintf(&b, "reason: %s\n", o.Reason)
	}
	if w == nil {
		b.WriteString("no witness\n")
		return b.String(), nil
	}
	fmt.Fprintf(&b, "witness: level %s, kind %s", w.Level, w.Kind)
	if w.Key != nil {
		fmt.Fprintf(&b, ", key %q", *w.Key)
	}
	b.WriteString("\n")
	switch w.Kind {
	case "order":
		fmt.Fprintf(&b, "order (%d operations):\n", len(w.Order))
	case "value":
	default:
		fmt.Fprintf(&b, "placed before the failure (%d operations):\n", len(w.Order))
	}
	for i, id := range w.Order {
		fmt.Fprintf(&b, "  %d. %s\n", i+1, describe(ops, id))
	}
	if len(w.Cycle) > 0 {
		fmt.Fprintf(&b, "cycle (%d edges):\n", len(w.Cycle))
		for _, e := range w.Cycle {
			fmt.Fprintf(&b, "  %s\n    -[%s]-> %s\n", describe(ops, e.From), e.Relation, describe(ops, e.To))
		}
	}
	if len(w.Blocked) > 0 {
		b.WriteString("blocked heads:\n")
		for _, h := range w.Blocked {
			fmt.Fprintf(&b, "  session %d: %s\n", h.Session, describe(ops, h.Op))
			switch h.Reason {
			case "precedence":
				for _, a := range h.After {
					fmt.Fprintf(&b, "    must follow unplaced %s (%s)\n", describe(ops, a.Op), a.Relation)
				}
			default:
				fmt.Fprintf(&b, "    rejected by the model: state %s, observed %s\n", ints(h.State), ints(h.Observed))
			}
		}
	}
	if v := w.Value; v != nil {
		fmt.Fprintf(&b, "value check %s: %s\n  uids %s\n", v.Reason, describe(ops, v.Op), ints(v.UIDs))
		if v.Other != nil {
			fmt.Fprintf(&b, "  other: %s\n", describe(ops, *v.Other))
		}
	}
	return b.String(), nil
}

// WitnessSignature hashes the shape of a violation: the witness with every
// operation replaced by its kind and key and every uid list by its length.
// The placed order is context rather than shape and is left out, a cycle is
// rotated to its least edge sequence, and blocked heads are taken in sorted
// order, so two runs with the same anomaly share a signature.
func WitnessSignature(model string, w *view.Witness, events []view.Event) string {
	ops := operations(events)
	name := func(id int64) string {
		if o, ok := ops[id]; ok {
			return o.kind + ":" + strconv.Quote(o.key)
		}
		return "?"
	}
	h := fnv.New64a()
	key := ""
	if w.Key != nil {
		key = strconv.Quote(*w.Key)
	}
	fmt.Fprintf(h, "%s|%s|%s|%s|", model, w.Level, w.Kind, key)

	edges := make([]string, len(w.Cycle))
	for i, e := range w.Cycle {
		edges[i] = name(e.From) + ">" + e.Relation + ">" + name(e.To)
	}
	best := strings.Join(edges, ";")
	for r := 1; r < len(edges); r++ {
		if s := strings.Join(append(append([]string{}, edges[r:]...), edges[:r]...), ";"); s < best {
			best = s
		}
	}
	fmt.Fprintf(h, "cycle:%s|", best)

	heads := make([]string, len(w.Blocked))
	for i, b := range w.Blocked {
		s := b.Reason + ":" + name(b.Op)
		if b.Reason == "precedence" {
			after := make([]string, len(b.After))
			for j, a := range b.After {
				after[j] = name(a.Op) + "/" + a.Relation
			}
			sort.Strings(after)
			s += "<" + strings.Join(after, ",")
		} else {
			s += fmt.Sprintf("<%d/%d", len(b.State), len(b.Observed))
		}
		heads[i] = s
	}
	sort.Strings(heads)
	fmt.Fprintf(h, "blocked:%s|", strings.Join(heads, ";"))

	if v := w.Value; v != nil {
		other := "-"
		if v.Other != nil {
			other = name(*v.Other)
		}
		fmt.Fprintf(h, "value:%s:%s:%d:%s", v.Reason, name(v.Op), len(v.UIDs), other)
	}
	return fmt.Sprintf("%016x", h.Sum64())
}
