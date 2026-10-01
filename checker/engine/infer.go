package engine

// Relation ranks of witness edges. When several relations give one ordered
// pair, the edge carries the smallest rank.
const (
	relSession  uint8 = 1
	relRealTime uint8 = 2
	relWW       uint8 = 3
	relWR       uint8 = 4
	relRW       uint8 = 5
)

var relationNames = [...]string{"", "session", "real_time", "ww", "wr", "rw"}

type edge struct {
	from, to int32
	rel      uint8
}

// groupEdge is an edge from every source to every target. Sources and
// targets are disjoint.
type groupEdge struct {
	sources []edge // from and rel used; to unused
	targets []int32
}

// keyEdges holds the inferred edges of one key, between operations of that
// key.
type keyEdges struct {
	pairs  []edge
	groups []groupEdge
}

// ValueFailure is a failed value check.
type ValueFailure struct {
	Op     int64
	Reason string
	UIDs   []int64
	Other  *int64
}

func (h *History) valueChecks() {
	type longest struct {
		list []int64
		from int32
	}
	var lon []longest
	var priors []map[string]int32
	if h.model == KV {
		lon = make([]longest, len(h.keys))
		for i := range lon {
			lon[i].from = -1
		}
	} else {
		priors = make([]map[string]int32, len(h.keys))
	}
	seen := make(map[int64]int)
	for i := range h.ops {
		o := &h.ops[i]
		if !o.completed || o.kind == Write {
			continue
		}
		l := h.obs(int32(i))

		var phantom []int64
		for _, u := range l {
			w, ok := h.uidOp[u]
			if !ok || h.ops[w].key != o.key {
				if !containsInt(phantom, u) {
					phantom = append(phantom, u)
				}
			}
		}
		if len(phantom) > 0 {
			h.value = &ValueFailure{Op: o.id, Reason: "phantom_uid", UIDs: phantom}
			return
		}

		clear(seen)
		var repeated []int64
		for _, u := range l {
			seen[u]++
			if seen[u] == 2 {
				repeated = append(repeated, u)
			}
		}
		if len(repeated) > 0 {
			h.value = &ValueFailure{Op: o.id, Reason: "repeated_uid", UIDs: repeated}
			return
		}

		if h.model == KV {
			lk := &lon[o.key]
			switch {
			case isPrefix(l, lk.list):
			case isPrefix(lk.list, l):
				lk.list, lk.from = l, int32(i)
			default:
				other := h.ops[lk.from].id
				h.value = &ValueFailure{Op: o.id, Reason: "not_prefix", UIDs: append([]int64{}, l...), Other: &other}
				return
			}
		} else if o.kind == RMW {
			m := priors[o.key]
			if m == nil {
				m = make(map[string]int32)
				priors[o.key] = m
			}
			s := listKey(l)
			if first, ok := m[s]; ok {
				other := h.ops[first].id
				h.value = &ValueFailure{Op: o.id, Reason: "same_prior", UIDs: append([]int64{}, l...), Other: &other}
				return
			}
			m[s] = int32(i)
		}
	}
}

func containsInt(xs []int64, x int64) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

func isPrefix(a, b []int64) bool {
	if len(a) > len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalList(a, b []int64) bool {
	return len(a) == len(b) && isPrefix(a, b)
}

func listKey(l []int64) string {
	b := make([]byte, 0, 8*len(l))
	for _, u := range l {
		x := uint64(u)
		b = append(b, byte(x), byte(x>>8), byte(x>>16), byte(x>>24),
			byte(x>>32), byte(x>>40), byte(x>>48), byte(x>>56))
	}
	return string(b)
}

// infer builds the inferred edges of every key. It runs only after the
// value checks pass, which guarantees every observed uid names a writer of
// the same key and, under kv, that each key's lists form a prefix chain.
func (h *History) infer() {
	h.inferred = make([]keyEdges, len(h.keys))
	for k := range h.keys {
		if h.model == KV {
			h.inferKV(int32(k))
		} else {
			h.inferKVRMW(int32(k))
		}
	}
}

func (h *History) inferKV(k int32) {
	e := &h.inferred[k]
	var longest []int64
	for _, i := range h.byKey[k] {
		if h.ops[i].kind == Read {
			if l := h.obs(i); len(l) > len(longest) {
				longest = l
			}
		}
	}
	m := len(longest)
	v := make([]int32, m)
	observed := make(map[int64]bool, m)
	for j, u := range longest {
		v[j] = h.uidOp[u]
		observed[u] = true
	}
	for j := 0; j+1 < m; j++ {
		e.pairs = append(e.pairs, edge{v[j], v[j+1], relWW})
	}
	var unobserved []int32
	for _, i := range h.byKey[k] {
		if h.ops[i].kind == Write && !observed[h.ops[i].uid] {
			unobserved = append(unobserved, i)
		}
	}
	var g groupEdge
	if m > 0 {
		g.sources = append(g.sources, edge{from: v[m-1], rel: relWW})
	}
	for _, i := range h.byKey[k] {
		if h.ops[i].kind != Read {
			continue
		}
		j := len(h.obs(i))
		if j > 0 {
			e.pairs = append(e.pairs, edge{v[j-1], i, relWR})
		}
		if j < m {
			e.pairs = append(e.pairs, edge{i, v[j], relRW})
		} else {
			g.sources = append(g.sources, edge{from: i, rel: relRW})
		}
	}
	if len(unobserved) > 0 && len(g.sources) > 0 {
		g.targets = unobserved
		e.groups = append(e.groups, g)
	}
}

func (h *History) inferKVRMW(k int32) {
	e := &h.inferred[k]
	var writers []int32
	priorOf := make(map[string]int32)
	for _, i := range h.byKey[k] {
		o := &h.ops[i]
		if o.kind == Read {
			continue
		}
		writers = append(writers, i)
		if o.kind == RMW && o.completed {
			priorOf[listKey(h.obs(i))] = i
		}
	}
	var emptyReads groupEdge
	for _, i := range h.byKey[k] {
		o := &h.ops[i]
		if !o.completed || o.kind == Write {
			continue
		}
		l := h.obs(i)
		if o.kind == RMW {
			if len(l) > 0 {
				e.pairs = append(e.pairs, edge{h.uidOp[l[len(l)-1]], i, relWW})
			} else {
				for _, w := range writers {
					if w != i {
						e.pairs = append(e.pairs, edge{i, w, relWW})
					}
				}
			}
			continue
		}
		if len(l) > 0 {
			e.pairs = append(e.pairs, edge{h.uidOp[l[len(l)-1]], i, relWR})
			if m, ok := priorOf[listKey(l)]; ok {
				e.pairs = append(e.pairs, edge{i, m, relRW})
			}
		} else {
			emptyReads.sources = append(emptyReads.sources, edge{from: i, rel: relRW})
		}
	}
	if len(emptyReads.sources) > 0 && len(writers) > 0 {
		emptyReads.targets = writers
		e.groups = append(e.groups, emptyReads)
	}
}

// forEachInferred calls f for every inferred edge of key k, groups expanded.
func (h *History) forEachInferred(k int32, f func(edge)) {
	e := &h.inferred[k]
	for _, p := range e.pairs {
		f(p)
	}
	for _, g := range e.groups {
		for _, s := range g.sources {
			for _, t := range g.targets {
				f(edge{s.from, t, s.rel})
			}
		}
	}
}
