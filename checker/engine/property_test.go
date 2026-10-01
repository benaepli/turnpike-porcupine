package engine

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// genOp is an operation of a generated history, in the test's own terms so
// the checks below do not depend on the engine's representation.
type genOp struct {
	id, client int64
	kind       Kind
	key        string
	uid        int64
	obs        []int64
	completed  bool
	call, ret  int
}

// genHistory draws a history from a random concurrent execution of the
// model, then perturbs some observed lists and leaves some last operations
// pending.
func genHistory(r *rand.Rand, m Model, maxOps, maxSessions, maxKeys int) []Row {
	nSess := 1 + r.Intn(maxSessions)
	clients := r.Perm(9)[:nSess]
	nOps := 1 + r.Intn(maxOps)
	keys := []string{"x", "y", "z"}[:1+r.Intn(maxKeys)]
	ids := r.Perm(nOps)
	uids := r.Perm(100)

	type plan struct {
		kind    Kind
		key     string
		id, uid int64
	}
	queues := make([][]plan, nSess)
	for i := 0; i < nOps; i++ {
		s := r.Intn(nSess)
		kinds := []Kind{Write, Read}
		if m == KVRMW {
			kinds = append(kinds, RMW)
		}
		p := plan{kind: kinds[r.Intn(len(kinds))], key: keys[r.Intn(len(keys))], id: int64(ids[i] + 1)}
		if p.kind != Read {
			p.uid = int64(100 + uids[i])
		}
		queues[s] = append(queues[s], p)
	}

	state := map[string][]int64{}
	var history [][]int64
	step := func(p plan) []int64 {
		cur := slices.Clone(state[p.key])
		history = append(history, cur)
		switch {
		case p.kind == Read:
			return cur
		case p.kind == RMW:
			state[p.key] = append(slices.Clone(cur), p.uid)
			return cur
		case m == KVRMW:
			state[p.key] = []int64{p.uid}
		default:
			state[p.key] = append(slices.Clone(cur), p.uid)
		}
		return nil
	}
	perturb := func(l []int64) []int64 {
		if r.Intn(4) != 0 {
			return l
		}
		switch r.Intn(5) {
		case 0:
			if len(l) > 0 {
				return l[:len(l)-1]
			}
		case 1:
			if len(l) > 0 {
				return l[1:]
			}
		case 2:
			if len(l) > 1 {
				l = slices.Clone(l)
				i := r.Intn(len(l) - 1)
				l[i], l[i+1] = l[i+1], l[i]
				return l
			}
		case 3:
			if len(history) > 0 {
				return history[r.Intn(len(history))]
			}
		case 4:
			return append(slices.Clone(l), int64(100+uids[r.Intn(nOps)]))
		}
		return l
	}

	type active struct {
		p       plan
		applied bool
		out     []int64
	}
	act := make([]*active, nSess)
	next := make([]int, nSess)
	pendingAllowed := make([]bool, nSess)
	for s := range pendingAllowed {
		pendingAllowed[s] = r.Intn(4) == 0
	}
	var rows []Row
	for {
		var ready []int
		for s := 0; s < nSess; s++ {
			if act[s] != nil || next[s] < len(queues[s]) {
				ready = append(ready, s)
			}
		}
		if len(ready) == 0 {
			break
		}
		s := ready[r.Intn(len(ready))]
		c := int64(clients[s] + 1)
		action := map[Kind]string{Write: "Client.Write", Read: "Client.Read", RMW: "Client.RMW"}
		if act[s] == nil {
			p := queues[s][next[s]]
			next[s]++
			a := &active{p: p}
			if r.Intn(2) == 0 {
				a.out, a.applied = step(p), true
			}
			act[s] = a
			row := Row{Kind: Invocation, ID: p.id, Client: c, Action: action[p.kind], Key: p.key, HasKey: true}
			if p.kind != Read {
				row.UID, row.HasUID = p.uid, true
			}
			rows = append(rows, row)
			if r.Intn(5) == 0 {
				rows = append(rows, Row{Kind: System, Action: "System.Crash"})
			}
			continue
		}
		a := act[s]
		if !a.applied {
			a.out = step(a.p)
		}
		act[s] = nil
		if next[s] == len(queues[s]) && pendingAllowed[s] {
			continue
		}
		row := Row{Kind: Response, ID: a.p.id, Client: c, Action: action[a.p.kind]}
		if a.p.kind != Write {
			row.Value, row.HasValue = perturb(a.out), true
		}
		rows = append(rows, row)
	}
	return rows
}

// genOps converts rows into operations with times, pending reads dropped.
func genOps(rows []Row) []genOp {
	var ops []genOp
	at := map[int64]int{}
	t := 0
	for _, r := range rows {
		if r.Kind == System {
			continue
		}
		t++
		k, _ := ActionKind(r.Action)
		if r.Kind == Invocation {
			at[r.ID] = len(ops)
			ops = append(ops, genOp{id: r.ID, client: r.Client, kind: k, key: r.Key, uid: r.UID, call: t})
			continue
		}
		o := &ops[at[r.ID]]
		o.completed, o.ret, o.obs = true, t, r.Value
	}
	var out []genOp
	for _, o := range ops {
		if !o.completed {
			if o.kind == Read {
				continue
			}
			o.ret = t + 1
		}
		out = append(out, o)
	}
	return out
}

// oracle holds the contract's edges for a generated history, built pair by
// pair from sections 4.2 to 4.4.
type oracle struct {
	m   Model
	l   KindSet
	ops []genOp
	inf map[[2]int]uint8
}

func newOracle(m Model, l KindSet, ops []genOp) *oracle {
	o := &oracle{m: m, l: l, ops: ops, inf: map[[2]int]uint8{}}
	add := func(a, b int, rel uint8) {
		if r, ok := o.inf[[2]int{a, b}]; !ok || rel < r {
			o.inf[[2]int{a, b}] = rel
		}
	}
	byUID := map[int64]int{}
	for i, x := range ops {
		if x.kind != Read {
			byUID[x.uid] = i
		}
	}
	keys := map[string]bool{}
	for _, x := range ops {
		keys[x.key] = true
	}
	for k := range keys {
		if m == KV {
			var longest []int64
			for _, x := range ops {
				if x.key == k && x.kind == Read && len(x.obs) > len(longest) {
					longest = x.obs
				}
			}
			mm := len(longest)
			v := func(i int) int { return byUID[longest[i-1]] }
			var unobserved []int
			for i, x := range ops {
				if x.key == k && x.kind == Write && !slices.Contains(longest, x.uid) {
					unobserved = append(unobserved, i)
				}
			}
			for i := 1; i < mm; i++ {
				add(v(i), v(i+1), relWW)
			}
			if mm > 0 {
				for _, u := range unobserved {
					add(v(mm), u, relWW)
				}
			}
			for i, x := range ops {
				if x.key != k || x.kind != Read {
					continue
				}
				j := len(x.obs)
				if j > 0 {
					add(v(j), i, relWR)
				}
				if j < mm {
					add(i, v(j+1), relRW)
				} else {
					for _, u := range unobserved {
						add(i, u, relRW)
					}
				}
			}
			continue
		}
		for i, x := range ops {
			if x.key != k || !x.completed || x.kind == Write {
				continue
			}
			if x.kind == RMW {
				if len(x.obs) > 0 {
					add(byUID[x.obs[len(x.obs)-1]], i, relWW)
				} else {
					for j, y := range ops {
						if j != i && y.key == k && y.kind != Read {
							add(i, j, relWW)
						}
					}
				}
				continue
			}
			if len(x.obs) > 0 {
				add(byUID[x.obs[len(x.obs)-1]], i, relWR)
				for j, y := range ops {
					if y.key == k && y.kind == RMW && y.completed && len(y.obs) > 0 && slices.Equal(y.obs, x.obs) {
						add(i, j, relRW)
					}
				}
			} else {
				for j, y := range ops {
					if y.key == k && y.kind != Read {
						add(i, j, relRW)
					}
				}
			}
		}
	}
	return o
}

// rank is the relation of the edge a -> b, or 0 when there is none.
func (o *oracle) rank(a, b int, inferred bool) uint8 {
	x, y := o.ops[a], o.ops[b]
	if x.client == y.client && x.call < y.call {
		return relSession
	}
	if o.l.Has(x.kind) && o.l.Has(y.kind) && x.ret < y.call {
		return relRealTime
	}
	if inferred {
		return o.inf[[2]int{a, b}]
	}
	return 0
}

type modelState map[string][]int64

func (o *oracle) step(st modelState, i int) (modelState, bool) {
	x := o.ops[i]
	cur := st[x.key]
	legal := true
	var next []int64
	switch {
	case x.kind == Read:
		return st, slices.Equal(x.obs, cur)
	case x.kind == RMW:
		legal = !x.completed || slices.Equal(x.obs, cur)
		next = append(slices.Clone(cur), x.uid)
	case o.m == KVRMW:
		next = []int64{x.uid}
	default:
		next = append(slices.Clone(cur), x.uid)
	}
	out := modelState{}
	for k, v := range st {
		out[k] = v
	}
	out[x.key] = next
	return out, legal
}

func (o *oracle) placeable(placed []bool, i int, inferred bool) bool {
	if placed[i] {
		return false
	}
	for a := range o.ops {
		if !placed[a] && a != i && o.rank(a, i, inferred) != 0 {
			return false
		}
	}
	return !inferred || o.inf[[2]int{i, i}] == 0
}

// holds enumerates every order the claim admits.
func (o *oracle) holds() bool {
	placed := make([]bool, len(o.ops))
	var rec func(st modelState, n int) bool
	rec = func(st modelState, n int) bool {
		if n == len(o.ops) {
			return true
		}
		for i := range o.ops {
			if !o.placeable(placed, i, false) {
				continue
			}
			next, ok := o.step(st, i)
			if !ok {
				continue
			}
			placed[i] = true
			if rec(next, n+1) {
				return true
			}
			placed[i] = false
		}
		return false
	}
	return rec(modelState{}, 0)
}

// deepest is the most operations any sequence of enabled, legal steps
// places.
func (o *oracle) deepest() int {
	placed := make([]bool, len(o.ops))
	best := 0
	var rec func(st modelState, n int)
	rec = func(st modelState, n int) {
		if n > best {
			best = n
		}
		for i := range o.ops {
			if !o.placeable(placed, i, true) {
				continue
			}
			next, ok := o.step(st, i)
			if !ok {
				continue
			}
			placed[i] = true
			rec(next, n+1)
			placed[i] = false
		}
	}
	rec(modelState{}, 0)
	return best
}

func (o *oracle) index(id int64) int {
	for i, x := range o.ops {
		if x.id == id {
			return i
		}
	}
	return -1
}

// replay places order, checking every step is enabled and legal.
func (o *oracle) replay(order []int64, inferred bool) ([]bool, modelState, error) {
	placed := make([]bool, len(o.ops))
	st := modelState{}
	for _, id := range order {
		i := o.index(id)
		if i < 0 {
			return nil, nil, fmt.Errorf("unknown operation %d", id)
		}
		if !o.placeable(placed, i, inferred) {
			return nil, nil, fmt.Errorf("operation %d placed before a predecessor", id)
		}
		next, ok := o.step(st, i)
		if !ok {
			return nil, nil, fmt.Errorf("operation %d's step is illegal", id)
		}
		st = next
		placed[i] = true
	}
	return placed, st, nil
}

// shortestCycleLen is the length of the shortest cycle among the unplaced
// operations, or 0.
func (o *oracle) shortestCycleLen(placed []bool) int {
	n := len(o.ops)
	const inf = 1 << 20
	d := make([][]int, n)
	for a := range d {
		d[a] = make([]int, n)
		for b := range d[a] {
			d[a][b] = inf
			if !placed[a] && !placed[b] && o.rank(a, b, true) != 0 {
				d[a][b] = 1
			}
		}
	}
	for k := 0; k < n; k++ {
		for a := 0; a < n; a++ {
			for b := 0; b < n; b++ {
				if d[a][k]+d[k][b] < d[a][b] {
					d[a][b] = d[a][k] + d[k][b]
				}
			}
		}
	}
	best := 0
	for a := 0; a < n; a++ {
		if d[a][a] < inf && (best == 0 || d[a][a] < best) {
			best = d[a][a]
		}
	}
	return best
}

func relRank(name string) uint8 {
	for i, n := range relationNames {
		if n == name && i > 0 {
			return uint8(i)
		}
	}
	return 0
}

// checkWitness verifies a witness against the oracle's edges.
func checkWitness(o *oracle, r ClaimResult) error {
	w := r.Witness
	if w == nil {
		return fmt.Errorf("no witness")
	}
	if w.Key != nil {
		var part []genOp
		for _, x := range o.ops {
			if x.key == *w.Key {
				part = append(part, x)
			}
		}
		o = newOracle(o.m, o.l, part)
	}
	switch w.Kind {
	case "value":
		return nil
	case "order":
		if len(w.Order) != len(o.ops) {
			return fmt.Errorf("order has %d operations, want %d", len(w.Order), len(o.ops))
		}
		_, _, err := o.replay(w.Order, false)
		return err
	case "cycle":
		placed, _, err := o.replay(w.Order, true)
		if err != nil {
			return err
		}
		for i := range o.ops {
			if o.placeable(placed, i, true) {
				return fmt.Errorf("operation %d is enabled at the stopping visit", o.ops[i].id)
			}
		}
		if len(w.Cycle) == 0 {
			return fmt.Errorf("empty cycle")
		}
		minID := w.Cycle[0].From
		for j, e := range w.Cycle {
			a, b := o.index(e.From), o.index(e.To)
			if a < 0 || b < 0 || placed[a] || placed[b] {
				return fmt.Errorf("edge %v leaves the unplaced operations", e)
			}
			if got := o.rank(a, b, true); got == 0 || relationNames[got] != e.Relation {
				return fmt.Errorf("edge %v: relation is %q", e, relationNames[got])
			}
			if next := w.Cycle[(j+1)%len(w.Cycle)]; next.From != e.To {
				return fmt.Errorf("edges %v and %v do not chain", e, next)
			}
			minID = min(minID, e.From)
		}
		if minID != w.Cycle[0].From {
			return fmt.Errorf("cycle does not start at its smallest id")
		}
		if want := o.shortestCycleLen(placed); len(w.Cycle) != want {
			return fmt.Errorf("cycle has %d edges, the shortest has %d", len(w.Cycle), want)
		}
		return nil
	case "frontier":
		placed, st, err := o.replay(w.Order, true)
		if err != nil {
			return err
		}
		if want := o.deepest(); len(w.Order) != want {
			return fmt.Errorf("deepest visit places %d, the search space reaches %d", len(w.Order), want)
		}
		var heads []int
		var sessions []int64
		for i, x := range o.ops {
			if placed[i] || slices.Contains(sessions, x.client) {
				continue
			}
			first := true
			for j, y := range o.ops {
				if !placed[j] && y.client == x.client && y.call < x.call {
					first = false
				}
			}
			if first {
				heads = append(heads, i)
				sessions = append(sessions, x.client)
			}
		}
		slices.SortFunc(heads, func(a, b int) int { return int(o.ops[a].client - o.ops[b].client) })
		if len(heads) != len(w.Blocked) {
			return fmt.Errorf("%d blocked heads, want %d", len(w.Blocked), len(heads))
		}
		for j, i := range heads {
			b := w.Blocked[j]
			if b.Op != o.ops[i].id || b.Session != o.ops[i].client {
				return fmt.Errorf("blocked head %d is %d of session %d, want %d", j, b.Op, b.Session, o.ops[i].id)
			}
			var want []Predecessor
			for a := range o.ops {
				if !placed[a] && a != i {
					if rk := o.rank(a, i, true); rk != 0 {
						want = append(want, Predecessor{o.ops[a].id, relationNames[rk]})
					}
				}
			}
			if o.inf[[2]int{i, i}] != 0 {
				want = append(want, Predecessor{o.ops[i].id, relationNames[o.inf[[2]int{i, i}]]})
			}
			slices.SortFunc(want, func(a, b Predecessor) int { return int(a.Op - b.Op) })
			if len(want) > 0 {
				if b.Reason != "precedence" || !slices.Equal(b.After, want) {
					return fmt.Errorf("head %d: got %+v, want precedence %+v", b.Op, b, want)
				}
				continue
			}
			x := o.ops[i]
			if b.Reason != "value" || !slices.Equal(b.State, st[x.key]) || !slices.Equal(b.Observed, x.obs) {
				return fmt.Errorf("head %d: got %+v, want value with state %v", b.Op, b, st[x.key])
			}
			if _, ok := o.step(st, i); ok {
				return fmt.Errorf("head %d is placeable", b.Op)
			}
		}
		return nil
	}
	return fmt.Errorf("unknown witness kind %q", w.Kind)
}

func allClaims(m Model) []Claim {
	var out []Claim
	d := m.Declared()
	for l := KindSet(0); l <= d; l++ {
		if l&^d == 0 {
			out = append(out, ClaimWith(m, l))
		}
	}
	return out
}

func describe(rows []Row) string {
	s := ""
	for _, r := range rows {
		switch r.Kind {
		case Invocation:
			s += fmt.Sprintf("  inv  id=%d c=%d %s key=%s uid=%d\n", r.ID, r.Client, r.Action, r.Key, r.UID)
		case Response:
			s += fmt.Sprintf("  resp id=%d c=%d %s value=%v\n", r.ID, r.Client, r.Action, r.Value)
		}
	}
	return s
}

// TestBruteForce compares every claim's verdict, the triage and the witness
// with enumeration of total orders on histories of seven operations or
// fewer.
func TestBruteForce(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	iterations := 4000
	if testing.Short() {
		iterations = 400
	}
	counts := map[string]int{}
	for it := 0; it < iterations; it++ {
		m := Model(it % 2)
		rows := genHistory(r, m, 7, 4, 2)
		h := Prepare(rows, m)
		if h.AdapterError() != "" {
			t.Fatalf("generated history rejected: %s\n%s", h.AdapterError(), describe(rows))
		}
		ops := genOps(rows)
		holds := map[KindSet]bool{}
		for _, c := range allClaims(m) {
			o := newOracle(m, c.Linearizable(), ops)
			want := o.holds()
			holds[c.Linearizable()] = want
			got := h.CheckClaim(c, Interrupt{})
			counts[c.Name()+"/"+got.Verdict]++
			if got.Verdict != map[bool]string{true: VerdictOK, false: VerdictIllegal}[want] {
				t.Fatalf("%s %s: verdict %s (%s), brute force holds=%v\n%s", m, c.Name(), got.Verdict, got.Reason, want, describe(rows))
			}
			if got.Witness.Level != c.Name() {
				t.Fatalf("witness level %q for claim %q", got.Witness.Level, c.Name())
			}
			if err := checkWitness(o, got); err != nil {
				t.Fatalf("%s %s %s: %v\n%s\n%s", m, c.Name(), got.Verdict, err, got.Witness, describe(rows))
			}
			if got.Witness.Kind == "value" && want {
				t.Fatalf("value failure on a history that holds")
			}
		}
		for _, c := range allClaims(m) {
			want := "none"
			for _, lvl := range Ladder(m) {
				if holds[lvl.Linearizable()] {
					want = lvl.Name()
					break
				}
			}
			res := h.CheckClaim(c, Interrupt{})
			if got := h.Triage(c, res, Interrupt{}); got != want {
				t.Fatalf("%s %s: triage %s, want %s\n%s", m, c.Name(), got, want, describe(rows))
			}
		}
	}
	t.Logf("verdicts: %v", counts)
}

// TestWithoutInference checks that the inferred edges never change a
// verdict, on histories too large to enumerate.
func TestWithoutInference(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	iterations := 3000
	if testing.Short() {
		iterations = 300
	}
	for it := 0; it < iterations; it++ {
		m := Model(it % 2)
		rows := genHistory(r, m, 14, 4, 2)
		h := Prepare(rows, m)
		ops := genOps(rows)
		for _, c := range allClaims(m) {
			with := h.CheckClaim(c, Interrupt{})
			withoutInference = true
			without := h.CheckClaim(c, Interrupt{})
			withoutInference = false
			if with.Verdict != without.Verdict {
				t.Fatalf("%s %s: %s with inferred edges, %s without\n%s", m, c.Name(), with.Verdict, without.Verdict, describe(rows))
			}
			o := newOracle(m, c.Linearizable(), ops)
			if with.Witness.Kind != "frontier" {
				if err := checkWitness(o, with); err != nil {
					t.Fatalf("%s %s: %v\n%s\n%s", m, c.Name(), err, with.Witness, describe(rows))
				}
			}
		}
	}
}

// TestGroupedEdgesCompile checks that compiling the grouped edges gives the
// same precedence as compiling every edge one pair at a time.
func TestGroupedEdgesCompile(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	for it := 0; it < 2000; it++ {
		m := Model(it % 2)
		h := Prepare(genHistory(r, m, 20, 5, 3), m)
		if h.value != nil {
			continue
		}
		for _, c := range allClaims(m) {
			rels := c.Relations()
			a := newView(h, -1)
			a.compile(rels, true, nil)
			var pairs []edge
			for k := range h.keys {
				h.forEachInferred(int32(k), func(e edge) { pairs = append(pairs, e) })
			}
			b := newView(h, -1)
			b.compile(rels, false, pairs)
			if !slices.Equal(a.req, b.req) {
				t.Fatalf("grouped and pairwise compilation differ")
			}
		}
	}
}

// TestMemoMatchesNoMemo checks the memo changes neither the verdict nor the
// witness.
func TestMemoMatchesNoMemo(t *testing.T) {
	r := rand.New(rand.NewSource(4))
	for it := 0; it < 1500; it++ {
		rows := genHistory(r, KVRMW, 12, 4, 2)
		h := Prepare(rows, KVRMW)
		for _, c := range allClaims(KVRMW) {
			a := h.CheckClaim(c, Interrupt{})
			withoutMemo = true
			b := h.CheckClaim(c, Interrupt{})
			withoutMemo = false
			if a.Verdict != b.Verdict || a.Witness.String() != b.Witness.String() {
				t.Fatalf("%s: memo %s %s, no memo %s %s\n%s", c.Name(), a.Verdict, a.Witness, b.Verdict, b.Witness, describe(rows))
			}
		}
	}
}
