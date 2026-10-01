package engine

import "sort"

// view is the history one search runs on: the whole history, or the
// operations of one key in partitioned mode.
type view struct {
	h   *History
	key int32 // -1 for the whole history
	ops []int32

	// Sessions in ascending client id; sessStart[s]..sessStart[s+1] indexes
	// sessOps, which holds each session's operations in call order.
	clients   []int64
	sessStart []int32
	sessOps   []int32

	// Indexed by global operation index; meaningful for operations in ops.
	sess []int32
	idx  []int32

	// req[local(o)*k + s] is how many operations of session s must be
	// placed before o is enabled.
	k     int
	local []int32
	req   []int32
}

func (v *view) keys() []int32 {
	if v.key >= 0 {
		return []int32{v.key}
	}
	ks := make([]int32, len(v.h.keys))
	for i := range ks {
		ks[i] = int32(i)
	}
	return ks
}

func (v *view) session(s int) []int32 {
	return v.sessOps[v.sessStart[s]:v.sessStart[s+1]]
}

func newView(h *History, key int32) *view {
	v := &view{h: h, key: key}
	if key >= 0 {
		v.ops = h.byKey[key]
	} else {
		v.ops = make([]int32, len(h.ops))
		for i := range v.ops {
			v.ops[i] = int32(i)
		}
	}
	n := len(h.ops)
	v.sess = make([]int32, n)
	v.idx = make([]int32, n)
	v.local = make([]int32, n)

	clientIdx := make(map[int64]int32)
	for _, o := range v.ops {
		c := h.ops[o].client
		if _, ok := clientIdx[c]; !ok {
			clientIdx[c] = 0
			v.clients = append(v.clients, c)
		}
	}
	sort.Slice(v.clients, func(a, b int) bool { return v.clients[a] < v.clients[b] })
	for s, c := range v.clients {
		clientIdx[c] = int32(s)
	}
	v.k = len(v.clients)
	counts := make([]int32, v.k+1)
	for li, o := range v.ops {
		s := clientIdx[h.ops[o].client]
		v.sess[o] = s
		v.idx[o] = counts[s+1]
		counts[s+1]++
		v.local[o] = int32(li)
	}
	v.sessStart = make([]int32, v.k+1)
	for s := 0; s < v.k; s++ {
		v.sessStart[s+1] = v.sessStart[s] + counts[s+1]
	}
	v.sessOps = make([]int32, len(v.ops))
	for _, o := range v.ops {
		s := v.sess[o]
		v.sessOps[v.sessStart[s]+v.idx[o]] = o
	}
	return v
}

func (v *view) need(to, from int32) {
	p := &v.req[int(v.local[to])*v.k+int(v.sess[from])]
	if n := v.idx[from] + 1; n > *p {
		*p = n
	}
}

// compile fills req from the relations, the inferred edges when inferred is
// set, and the extra edges.
func (v *view) compile(rels []Relation, inferred bool, extra []edge) {
	h := v.h
	v.req = make([]int32, len(v.ops)*v.k)
	for _, r := range rels {
		if r.Session {
			continue
		}
		v.compileRealTime(r.From, r.To)
	}
	if inferred {
		maxSrc := make([]int32, v.k)
		for _, key := range v.keys() {
			e := &h.inferred[key]
			for _, p := range e.pairs {
				v.need(p.to, p.from)
			}
			for _, g := range e.groups {
				clear(maxSrc)
				for _, s := range g.sources {
					if n := v.idx[s.from] + 1; n > maxSrc[v.sess[s.from]] {
						maxSrc[v.sess[s.from]] = n
					}
				}
				for _, t := range g.targets {
					row := v.req[int(v.local[t])*v.k:]
					for s, n := range maxSrc {
						if n > row[s] {
							row[s] = n
						}
					}
				}
			}
		}
	}
	for _, p := range extra {
		v.need(p.to, p.from)
	}
}

// compileRealTime adds, for every operation o with kind in to and every
// session s, the last operation of s with kind in from that returned before
// o was invoked. Returns increase along a session, so a binary search over
// the session's returns finds it.
func (v *view) compileRealTime(from, to KindSet) {
	h := v.h
	rets := make([][]int32, v.k)
	lastFrom := make([][]int32, v.k)
	for s := 0; s < v.k; s++ {
		ops := v.session(s)
		rets[s] = make([]int32, len(ops))
		lastFrom[s] = make([]int32, len(ops))
		last := int32(-1)
		for i, o := range ops {
			rets[s][i] = h.ops[o].ret
			if from.Has(h.ops[o].kind) {
				last = int32(i)
			}
			lastFrom[s][i] = last
		}
	}
	for _, o := range v.ops {
		if !to.Has(h.ops[o].kind) {
			continue
		}
		call := h.ops[o].call
		row := v.req[int(v.local[o])*v.k:]
		for s := 0; s < v.k; s++ {
			rs := rets[s]
			j := sort.Search(len(rs), func(i int) bool { return rs[i] >= call })
			if j == 0 {
				continue
			}
			if t := lastFrom[s][j-1]; t >= 0 && t+1 > row[s] {
				row[s] = t + 1
			}
		}
	}
}
