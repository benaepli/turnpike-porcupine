package engine

import "sort"

const (
	VerdictOK      = "ok"
	VerdictIllegal = "illegal"
	VerdictUnknown = "unknown"
)

// ClaimResult is the verdict of one claim. Reason is empty for a definitive
// verdict and otherwise starts with adapter_error, timeout, cancelled or
// claim_internal. Witness is nil exactly when the verdict is unknown.
type ClaimResult struct {
	Verdict string
	Reason  string
	Witness *Witness
}

// Result is a claim's verdict together with the history's triage: the
// strongest ladder level that holds, "none", or "unknown".
type Result struct {
	Verdict string
	Reason  string
	Triage  string
	Witness *Witness
}

// Check prepares rows under the claim's model, checks the claim and walks
// the ladder for the triage, every stage under the same interrupt.
func Check(rows []Row, c Claim, intr Interrupt) Result {
	h := Prepare(rows, c.Model)
	r := h.CheckClaim(c, intr)
	return Result{Verdict: r.Verdict, Reason: r.Reason, Triage: h.Triage(c, r, intr), Witness: r.Witness}
}

func unknown(reason string) ClaimResult {
	return ClaimResult{Verdict: VerdictUnknown, Reason: reason}
}

// Ops is the number of operations the check covers, pending reads excluded.
func (h *History) Ops() int { return len(h.ops) }

// AdapterError is the reason the rows could not be checked, or empty.
func (h *History) AdapterError() string { return h.adapterErr }

// CheckClaim checks one claim.
func (h *History) CheckClaim(c Claim, intr Interrupt) ClaimResult {
	if h.adapterErr != "" {
		return unknown("adapter_error: " + h.adapterErr)
	}
	name := c.Name()
	if h.value != nil {
		return ClaimResult{Verdict: VerdictIllegal, Witness: &Witness{Level: name, Kind: "value", Value: h.value}}
	}
	rels := c.Relations()
	if !partitioned(h.model, rels) {
		v := newView(h, -1)
		v.compile(rels, !withoutInference, nil)
		return h.runView(v, c, intr).ClaimResult
	}

	orders := make([][]int32, len(h.keys))
	firstUndecided := ""
	for _, k := range h.keyOrd {
		v := newView(h, k)
		v.compile(rels, !withoutInference, nil)
		r := h.runView(v, c, intr)
		switch r.Verdict {
		case VerdictIllegal:
			key := h.keys[k]
			r.Witness.Key = &key
			return r.ClaimResult
		case VerdictUnknown:
			if firstUndecided == "" {
				firstUndecided = r.Reason
			}
		default:
			orders[k] = r.order
		}
	}
	if firstUndecided != "" {
		return unknown(firstUndecided)
	}
	order, ok := h.mergeOrders(orders)
	if !ok {
		return unknown("claim_internal: the per-key orders do not merge")
	}
	return ClaimResult{Verdict: VerdictOK, Witness: &Witness{Level: name, Kind: "order", Order: h.ids(order)}}
}

type viewResult struct {
	ClaimResult
	order []int32
}

func (h *History) runView(v *view, c Claim, intr Interrupt) viewResult {
	s := newSearch(v, intr)
	var st status
	if s.kvPath {
		st = s.runKV()
	} else {
		st = s.runDFS()
	}
	name := c.Name()
	switch st {
	case statusOK:
		return viewResult{ClaimResult{Verdict: VerdictOK, Witness: &Witness{Level: name, Kind: "order", Order: h.ids(s.order)}}, s.order}
	case statusUndecided, statusInternal:
		return viewResult{ClaimResult: unknown(s.reason)}
	}
	if s.kvPath {
		cycle, ok := shortestCycle(v, c.Linearizable(), s.order)
		if !ok {
			return viewResult{ClaimResult: unknown("claim_internal: the search stopped with no cycle among the unplaced operations")}
		}
		return viewResult{ClaimResult: ClaimResult{Verdict: VerdictIllegal, Witness: &Witness{
			Level: name, Kind: "cycle", Order: h.ids(s.order), Cycle: cycle}}}
	}
	blocked, ok := blockedHeads(v, c.Linearizable(), s.deepOrder)
	if !ok {
		return viewResult{ClaimResult: unknown("claim_internal: a head at the deepest visit is placeable")}
	}
	return viewResult{ClaimResult: ClaimResult{Verdict: VerdictIllegal, Witness: &Witness{
		Level: name, Kind: "frontier", Order: h.ids(s.deepOrder), Blocked: blocked}}}
}

// blockedHeads replays the deepest visit and explains, for each session
// with operations left, why its head could not be placed.
func blockedHeads(v *view, l KindSet, placed []int32) ([]BlockedHead, bool) {
	h := v.h
	s := newSearch(v, Interrupt{})
	for _, o := range placed {
		s.apply(o)
	}
	_, in := unplaced(v, placed)

	heads := make(map[int32]map[int32]uint8)
	for sess := 0; sess < v.k; sess++ {
		if o := s.head(sess); o >= 0 {
			heads[o] = make(map[int32]uint8)
		}
	}
	note := func(from, to int32, rel uint8) {
		m, ok := heads[to]
		if !ok || !in[from] {
			return
		}
		if r, seen := m[from]; !seen || rel < r {
			m[from] = rel
		}
	}
	for o := range heads {
		if !l.Has(h.ops[o].kind) {
			continue
		}
		for _, u := range v.ops {
			if in[u] && l.Has(h.ops[u].kind) && h.ops[u].ret < h.ops[o].call {
				note(u, o, relRealTime)
			}
		}
	}
	for _, k := range v.keys() {
		h.forEachInferred(k, func(e edge) { note(e.from, e.to, e.rel) })
	}

	var out []BlockedHead
	for sess := 0; sess < v.k; sess++ {
		o := s.head(sess)
		if o < 0 {
			continue
		}
		p := &h.ops[o]
		b := BlockedHead{Session: v.clients[sess], Op: p.id}
		if preds := heads[o]; len(preds) > 0 {
			b.Reason = "precedence"
			for u, r := range preds {
				b.After = append(b.After, Predecessor{h.ops[u].id, relationNames[r]})
			}
			sort.Slice(b.After, func(i, j int) bool { return b.After[i].Op < b.After[j].Op })
		} else {
			if s.legal(o) {
				return nil, false
			}
			b.Reason = "value"
			b.State = append([]int64{}, s.value(p.key)...)
			b.Observed = append([]int64{}, h.obs(o)...)
		}
		out = append(out, b)
	}
	return out, true
}

// mergeOrders interleaves the per-key orders of a partitioned check into one
// order of the whole history: an operation is enabled when its session
// predecessors, every operation that returned before it was invoked, and its
// predecessor in its key's order are placed, and the enabled head with the
// earliest call is placed next.
func (h *History) mergeOrders(orders [][]int32) ([]int32, bool) {
	if len(h.keys) == 1 {
		// One key's order chains every operation, so the walk can only
		// reproduce it.
		return orders[0], true
	}
	v := newView(h, -1)
	var chain []edge
	for _, p := range orders {
		for i := 1; i < len(p); i++ {
			chain = append(chain, edge{from: p[i-1], to: p[i]})
		}
	}
	d := h.model.Declared()
	v.compile([]Relation{{Session: true}, {From: d, To: d}}, false, chain)
	f := make([]int32, v.k)
	order := make([]int32, 0, len(v.ops))
	for len(order) < len(v.ops) {
		best := int32(-1)
		for sess := 0; sess < v.k; sess++ {
			ops := v.session(sess)
			if int(f[sess]) >= len(ops) {
				continue
			}
			o := ops[f[sess]]
			if best >= 0 && o > best {
				continue
			}
			row := v.req[int(v.local[o])*v.k : int(v.local[o]+1)*v.k]
			enabled := true
			for i, n := range row {
				if f[i] < n {
					enabled = false
					break
				}
			}
			if enabled {
				best = o
			}
		}
		if best < 0 {
			return nil, false
		}
		f[v.sess[best]]++
		order = append(order, best)
	}
	return order, true
}

// Triage walks the ladder, strongest level first, and returns the first
// level that holds, "none" when every level fails, or "unknown" when an
// undecided level comes before any that holds. r is the claim's own result;
// a level the claim decides by implication is not checked again.
func (h *History) Triage(c Claim, r ClaimResult, intr Interrupt) string {
	lc := c.Linearizable()
	for _, lvl := range Ladder(h.model) {
		ll := lvl.Linearizable()
		var verdict string
		switch {
		case ll == lc:
			verdict = r.Verdict
		case r.Verdict == VerdictOK && ll&^lc == 0:
			verdict = VerdictOK
		case r.Verdict == VerdictIllegal && lc&^ll == 0:
			verdict = VerdictIllegal
		default:
			verdict = h.CheckClaim(lvl, intr).Verdict
		}
		switch verdict {
		case VerdictOK:
			return lvl.Name()
		case VerdictUnknown:
			return VerdictUnknown
		}
	}
	return "none"
}
