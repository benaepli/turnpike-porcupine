package engine

import (
	"sort"
	"strconv"
)

// Witness explains a definitive verdict. Operations are named by their ids.
type Witness struct {
	Level string
	// Kind is "order", "cycle", "frontier" or "value".
	Kind    string
	Key     *string
	Order   []int64
	Cycle   []CycleEdge
	Blocked []BlockedHead
	Value   *ValueFailure
}

type CycleEdge struct {
	From, To int64
	Relation string
}

// BlockedHead is a session's next operation at the deepest visit and why it
// could not be placed: Reason "precedence" with After, or "value" with State
// and Observed.
type BlockedHead struct {
	Session  int64
	Op       int64
	Reason   string
	After    []Predecessor
	State    []int64
	Observed []int64
}

type Predecessor struct {
	Op       int64
	Relation string
}

// String is the canonical serialization: one line of JSON, fields in a
// fixed order, no whitespace outside strings.
func (w *Witness) String() string {
	return string(w.AppendCanonical(nil))
}

func (w *Witness) AppendCanonical(b []byte) []byte {
	b = append(b, `{"level":`...)
	b = appendString(b, w.Level)
	b = append(b, `,"kind":`...)
	b = appendString(b, w.Kind)
	b = append(b, `,"key":`...)
	if w.Key == nil {
		b = append(b, "null"...)
	} else {
		b = appendString(b, *w.Key)
	}
	b = append(b, `,"order":`...)
	b = appendInts(b, w.Order)
	b = append(b, `,"cycle":[`...)
	for i, e := range w.Cycle {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, `{"from":`...)
		b = strconv.AppendInt(b, e.From, 10)
		b = append(b, `,"to":`...)
		b = strconv.AppendInt(b, e.To, 10)
		b = append(b, `,"relation":`...)
		b = appendString(b, e.Relation)
		b = append(b, '}')
	}
	b = append(b, `],"blocked":[`...)
	for i, h := range w.Blocked {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, `{"session":`...)
		b = strconv.AppendInt(b, h.Session, 10)
		b = append(b, `,"op":`...)
		b = strconv.AppendInt(b, h.Op, 10)
		b = append(b, `,"reason":`...)
		b = appendString(b, h.Reason)
		if h.Reason == "precedence" {
			b = append(b, `,"after":[`...)
			for j, p := range h.After {
				if j > 0 {
					b = append(b, ',')
				}
				b = append(b, `{"op":`...)
				b = strconv.AppendInt(b, p.Op, 10)
				b = append(b, `,"relation":`...)
				b = appendString(b, p.Relation)
				b = append(b, '}')
			}
			b = append(b, ']')
		} else {
			b = append(b, `,"state":`...)
			b = appendInts(b, h.State)
			b = append(b, `,"observed":`...)
			b = appendInts(b, h.Observed)
		}
		b = append(b, '}')
	}
	b = append(b, `],"value":`...)
	if w.Value == nil {
		b = append(b, "null"...)
	} else {
		v := w.Value
		b = append(b, `{"op":`...)
		b = strconv.AppendInt(b, v.Op, 10)
		b = append(b, `,"reason":`...)
		b = appendString(b, v.Reason)
		b = append(b, `,"uids":`...)
		b = appendInts(b, v.UIDs)
		b = append(b, `,"other":`...)
		if v.Other == nil {
			b = append(b, "null"...)
		} else {
			b = strconv.AppendInt(b, *v.Other, 10)
		}
		b = append(b, '}')
	}
	return append(b, '}')
}

func appendInts(b []byte, xs []int64) []byte {
	b = append(b, '[')
	for i, x := range xs {
		if i > 0 {
			b = append(b, ',')
		}
		b = strconv.AppendInt(b, x, 10)
	}
	return append(b, ']')
}

// appendString escapes only the quote, the backslash and code points below
// U+0020; every other byte is copied, so U+2028, U+2029, '<', '>' and '&'
// stay unescaped.
func appendString(b []byte, s string) []byte {
	const hex = "0123456789abcdef"
	b = append(b, '"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"':
			b = append(b, '\\', '"')
		case '\\':
			b = append(b, '\\', '\\')
		case '\b':
			b = append(b, '\\', 'b')
		case '\f':
			b = append(b, '\\', 'f')
		case '\n':
			b = append(b, '\\', 'n')
		case '\r':
			b = append(b, '\\', 'r')
		case '\t':
			b = append(b, '\\', 't')
		default:
			if c < 0x20 {
				b = append(b, '\\', 'u', '0', '0', hex[c>>4], hex[c&0xf])
			} else {
				b = append(b, c)
			}
		}
	}
	return append(b, '"')
}

func (h *History) ids(ops []int32) []int64 {
	out := make([]int64, len(ops))
	for i, o := range ops {
		out[i] = h.ops[o].id
	}
	return out
}

// unplaced returns the view's operations not in placed, and a membership
// table indexed by global operation index.
func unplaced(v *view, placed []int32) ([]int32, []bool) {
	in := make([]bool, len(v.h.ops))
	for _, o := range v.ops {
		in[o] = true
	}
	for _, o := range placed {
		in[o] = false
	}
	var out []int32
	for _, o := range v.ops {
		if in[o] {
			out = append(out, o)
		}
	}
	return out, in
}

// shortestCycle finds the shortest cycle among the unplaced operations,
// breadth-first from each operation in ascending id, neighbours in
// (relation rank, id) order, keeping the first cycle strictly shorter than
// every earlier one. A breadth-first search from s can only close a cycle
// inside the strongly connected component of s, so it starts only at
// operations on some cycle and explores only s's component; neither changes
// which cycle is kept.
func shortestCycle(v *view, l KindSet, placed []int32) ([]CycleEdge, bool) {
	h := v.h
	us, in := unplaced(v, placed)
	sort.Slice(us, func(a, b int) bool { return h.ops[us[a]].id < h.ops[us[b]].id })
	n := len(us)
	pos := make([]int32, len(h.ops))
	for i, o := range us {
		pos[o] = int32(i)
	}

	type out struct {
		to  int32
		rel uint8
	}
	inferred := make([][]out, n)
	for _, k := range v.keys() {
		h.forEachInferred(k, func(e edge) {
			if in[e.from] && in[e.to] {
				inferred[pos[e.from]] = append(inferred[pos[e.from]], out{pos[e.to], e.rel})
			}
		})
	}

	start := make([]int32, n+1)
	var tgt []int32
	var rel []uint8
	var sessBuf []int32
	for i, x := range us {
		ox := &h.ops[x]
		sessBuf = sessBuf[:0]
		for _, y := range v.session(int(v.sess[x]))[v.idx[x]+1:] {
			if in[y] {
				sessBuf = append(sessBuf, pos[y])
			}
		}
		sort.Slice(sessBuf, func(a, b int) bool { return sessBuf[a] < sessBuf[b] })
		for _, y := range sessBuf {
			tgt = append(tgt, y)
			rel = append(rel, relSession)
		}
		if l.Has(ox.kind) {
			for j, y := range us {
				oy := &h.ops[y]
				if l.Has(oy.kind) && ox.ret < oy.call {
					tgt = append(tgt, int32(j))
					rel = append(rel, relRealTime)
				}
			}
		}
		inf := inferred[i]
		sort.Slice(inf, func(a, b int) bool {
			if inf[a].rel != inf[b].rel {
				return inf[a].rel < inf[b].rel
			}
			return inf[a].to < inf[b].to
		})
		for _, e := range inf {
			tgt = append(tgt, e.to)
			rel = append(rel, e.rel)
		}
		start[i+1] = int32(len(tgt))
	}

	comp, cyclic := components(n, start, tgt)

	best := -1
	var bestEdges []CycleEdge
	depth := make([]int32, n)
	parent := make([]int32, n)
	parentRel := make([]uint8, n)
	stamp := make([]int32, n)
	queue := make([]int32, 0, n)
	for sNode := int32(0); sNode < int32(n); sNode++ {
		if !cyclic[sNode] {
			continue
		}
		mark := sNode + 1
		stamp[sNode] = mark
		depth[sNode] = 0
		queue = append(queue[:0], sNode)
		found := false
		for qi := 0; qi < len(queue) && !found; qi++ {
			x := queue[qi]
			if best >= 0 && int(depth[x])+1 >= best {
				break
			}
			for e := start[x]; e < start[x+1]; e++ {
				y := tgt[e]
				if y == sNode {
					var path []int32
					for z := x; z != sNode; z = parent[z] {
						path = append(path, z)
					}
					edges := make([]CycleEdge, 0, len(path)+1)
					prev := sNode
					for j := len(path) - 1; j >= 0; j-- {
						z := path[j]
						edges = append(edges, CycleEdge{h.ops[us[prev]].id, h.ops[us[z]].id, relationNames[parentRel[z]]})
						prev = z
					}
					edges = append(edges, CycleEdge{h.ops[us[x]].id, h.ops[us[sNode]].id, relationNames[rel[e]]})
					best = len(edges)
					bestEdges = edges
					found = true
					break
				}
				if comp[y] != comp[sNode] || stamp[y] == mark {
					continue
				}
				stamp[y] = mark
				depth[y] = depth[x] + 1
				parent[y] = x
				parentRel[y] = rel[e]
				queue = append(queue, y)
			}
		}
	}
	return bestEdges, best > 0
}

// components labels strongly connected components and reports, per node,
// whether it lies on a cycle.
func components(n int, start, tgt []int32) ([]int32, []bool) {
	index := make([]int32, n)
	low := make([]int32, n)
	onStack := make([]bool, n)
	comp := make([]int32, n)
	cyclic := make([]bool, n)
	for i := range index {
		index[i] = -1
	}
	var stack []int32
	type frame struct {
		node int32
		next int32
	}
	var call []frame
	counter := int32(0)
	ncomp := int32(0)
	for root := int32(0); root < int32(n); root++ {
		if index[root] >= 0 {
			continue
		}
		call = append(call, frame{root, start[root]})
		index[root], low[root] = counter, counter
		counter++
		stack = append(stack, root)
		onStack[root] = true
		for len(call) > 0 {
			top := &call[len(call)-1]
			x := top.node
			if top.next < start[x+1] {
				y := tgt[top.next]
				top.next++
				if y == x {
					cyclic[x] = true
				}
				if index[y] < 0 {
					index[y], low[y] = counter, counter
					counter++
					stack = append(stack, y)
					onStack[y] = true
					call = append(call, frame{y, start[y]})
				} else if onStack[y] && index[y] < low[x] {
					low[x] = index[y]
				}
				continue
			}
			call = call[:len(call)-1]
			if len(call) > 0 {
				p := call[len(call)-1].node
				if low[x] < low[p] {
					low[p] = low[x]
				}
			}
			if low[x] == index[x] {
				at := len(stack) - 1
				for stack[at] != x {
					at--
				}
				members := stack[at:]
				for _, y := range members {
					onStack[y] = false
					comp[y] = ncomp
					if len(members) > 1 {
						cyclic[y] = true
					}
				}
				stack = stack[:at]
				ncomp++
			}
		}
	}
	return comp, cyclic
}
