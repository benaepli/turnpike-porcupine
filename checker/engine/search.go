package engine

import (
	"bytes"
	"encoding/binary"
	"sync/atomic"
	"time"
)

// Interrupt bounds a check. The deadline and the flag are read before the
// first expansion of each search and then every 1024 expansions.
type Interrupt struct {
	// Deadline is the time after which a search stops; zero means none.
	Deadline time.Time
	// Cancel stops a search when set; nil means never.
	Cancel *atomic.Bool
}

func (i Interrupt) poll() string {
	if i.Cancel != nil && i.Cancel.Load() {
		return "cancelled"
	}
	if !i.Deadline.IsZero() && !time.Now().Before(i.Deadline) {
		return "timeout"
	}
	return ""
}

// withoutInference makes every search a full depth-first search over the
// claim's relations alone. Only tests set it: the inferred edges are a pure
// optimization, and the two searches must agree on every verdict.
var withoutInference bool

// withoutMemo disables memo lookups. Only tests set it: the memo must change
// neither a verdict nor a witness.
var withoutMemo bool

type status uint8

const (
	statusOK status = iota
	statusFail
	statusUndecided
	statusInternal
)

// search is one depth-first search over frontiers of a compiled view.
type search struct {
	v      *view
	h      *History
	kvPath bool
	intr   Interrupt

	f     []int32
	order []int32
	total int

	// Model state per key: the uid stack, and under kv_rmw the start of the
	// current value within it, so a blind write is undone by two pops.
	stacks [][]int64
	segs   [][]int32
	hashes [][]uint64

	expansions int
	reason     string

	// The deepest visit's placement order; deepOrder[:lcp] equals
	// order[:lcp].
	deepest   int
	deepOrder []int32
	lcp       int

	cands []int32

	memo memo
}

func newSearch(v *view, intr Interrupt) *search {
	h := v.h
	s := &search{v: v, h: h, kvPath: h.model == KV && !withoutInference, intr: intr, total: len(v.ops), deepest: -1}
	s.f = make([]int32, v.k)
	s.stacks = make([][]int64, len(h.keys))
	s.segs = make([][]int32, len(h.keys))
	s.hashes = make([][]uint64, len(h.keys))
	for _, k := range v.keys() {
		s.segs[k] = []int32{0}
	}
	if !s.kvPath {
		s.memo.init(v)
	}
	return s
}

func (s *search) head(sess int) int32 {
	v := s.v
	if int(s.f[sess]) >= int(v.sessStart[sess+1]-v.sessStart[sess]) {
		return -1
	}
	return v.sessOps[v.sessStart[sess]+s.f[sess]]
}

func (s *search) enabled(o int32) bool {
	v := s.v
	row := v.req[int(v.local[o])*v.k : int(v.local[o]+1)*v.k]
	for i, n := range row {
		if s.f[i] < n {
			return false
		}
	}
	return true
}

// value is the current state of key k.
func (s *search) value(k int32) []int64 {
	st := s.stacks[k]
	return st[s.segs[k][len(s.segs[k])-1]:]
}

func (s *search) legal(o int32) bool {
	p := &s.h.ops[o]
	switch p.kind {
	case Read:
		return equalList(s.h.obs(o), s.value(p.key))
	case RMW:
		return !p.completed || equalList(s.h.obs(o), s.value(p.key))
	}
	return true
}

const (
	hashSeed  uint64 = 0x9e3779b97f4a7c15
	hashEmpty uint64 = 0x243f6a8885a308d3
)

func mix(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

func (s *search) valueHash(k int32) uint64 {
	if len(s.value(k)) == 0 {
		return hashEmpty
	}
	hs := s.hashes[k]
	return hs[len(hs)-1]
}

func (s *search) keyTerm(k int32) uint64 {
	return mix(uint64(k)*hashSeed ^ s.valueHash(k))
}

func frontTerm(sess int, n int32) uint64 {
	return mix(uint64(sess)<<32 | uint64(uint32(n)) ^ hashEmpty)
}

func (s *search) apply(o int32) {
	p := &s.h.ops[o]
	k := p.key
	if !s.kvPath {
		s.memo.state -= s.keyTerm(k)
		sess := int(s.v.sess[o])
		s.memo.front += frontTerm(sess, s.f[sess]+1) - frontTerm(sess, s.f[sess])
	}
	if p.kind == Write && s.h.model == KVRMW {
		s.segs[k] = append(s.segs[k], int32(len(s.stacks[k])))
		s.stacks[k] = append(s.stacks[k], p.uid)
		if !s.kvPath {
			s.hashes[k] = append(s.hashes[k], mix(hashSeed^uint64(p.uid)))
		}
	} else if p.kind != Read {
		if !s.kvPath {
			s.hashes[k] = append(s.hashes[k], mix(s.valueHash(k)*hashSeed^uint64(p.uid)))
		}
		s.stacks[k] = append(s.stacks[k], p.uid)
	}
	if !s.kvPath {
		s.memo.state += s.keyTerm(k)
	}
	s.f[s.v.sess[o]]++
	s.order = append(s.order, o)
}

func (s *search) undo(o int32) {
	p := &s.h.ops[o]
	k := p.key
	sess := int(s.v.sess[o])
	s.f[sess]--
	s.order = s.order[:len(s.order)-1]
	if len(s.order) < s.lcp {
		s.lcp = len(s.order)
	}
	if !s.kvPath {
		s.memo.state -= s.keyTerm(k)
		s.memo.front -= frontTerm(sess, s.f[sess]+1) - frontTerm(sess, s.f[sess])
	}
	if p.kind != Read {
		s.stacks[k] = s.stacks[k][:len(s.stacks[k])-1]
		if !s.kvPath {
			s.hashes[k] = s.hashes[k][:len(s.hashes[k])-1]
		}
		if p.kind == Write && s.h.model == KVRMW {
			s.segs[k] = s.segs[k][:len(s.segs[k])-1]
		}
	}
	if !s.kvPath {
		s.memo.state += s.keyTerm(k)
	}
}

// expand counts one expansion, reading the interrupts first when the count
// is a multiple of 1024. It reports false when the search must stop.
func (s *search) expand() bool {
	if s.expansions%1024 == 0 {
		if r := s.intr.poll(); r != "" {
			s.reason = r
			return false
		}
	}
	s.expansions++
	return true
}

// runKV is the search on the kv path. With the inferred edges every enabled
// head is legal and no visit backtracks, so the search stops at its first
// visit with no candidate.
func (s *search) runKV() status {
	v := s.v
	for {
		if len(s.order) == s.total {
			return statusOK
		}
		if !s.expand() {
			return statusUndecided
		}
		best := int32(-1)
		for sess := 0; sess < v.k; sess++ {
			o := s.head(sess)
			if o >= 0 && (best < 0 || o < best) && s.enabled(o) {
				best = o
			}
		}
		if best < 0 {
			return statusFail
		}
		if !s.legal(best) {
			s.reason = "claim_internal: an enabled operation's step was rejected"
			return statusInternal
		}
		s.apply(best)
	}
}

func (s *search) runDFS() status {
	s.cands = make([]int32, 0, (s.total+1)*s.v.k)
	return s.visit()
}

func (s *search) visit() status {
	if len(s.order) == s.total {
		return statusOK
	}
	if s.memo.contains(s) {
		return statusFail
	}
	if !s.expand() {
		return statusUndecided
	}
	if len(s.order) > s.deepest {
		s.deepest = len(s.order)
		s.deepOrder = append(s.deepOrder[:s.lcp], s.order[s.lcp:]...)
		s.lcp = len(s.order)
	}
	base := len(s.cands)
	for sess := 0; sess < s.v.k; sess++ {
		o := s.head(sess)
		if o < 0 {
			continue
		}
		i := len(s.cands)
		s.cands = append(s.cands, o)
		for i > base && s.cands[i-1] > o {
			s.cands[i] = s.cands[i-1]
			i--
		}
		s.cands[i] = o
	}
	end := len(s.cands)
	for i := base; i < end; i++ {
		o := s.cands[i]
		if !s.enabled(o) || !s.legal(o) {
			continue
		}
		s.apply(o)
		if r := s.visit(); r != statusFail {
			return r
		}
		s.undo(o)
	}
	s.cands = s.cands[:base]
	s.memo.add(s)
	return statusFail
}

// memo remembers the visits whose every candidate failed. It is exact: an
// entry is the frontier and every key's value, and a hash only selects the
// entries compared.
type memo struct {
	keys  []int32
	front uint64
	state uint64

	heads map[uint64]int32
	next  []int32
	start []int32
	data  []byte
	buf   []byte
}

func (m *memo) init(v *view) {
	m.keys = v.keys()
	m.heads = make(map[uint64]int32)
	for sess := 0; sess < v.k; sess++ {
		m.front += frontTerm(sess, 0)
	}
	for _, k := range m.keys {
		m.state += mix(uint64(k)*hashSeed ^ hashEmpty)
	}
	m.start = append(m.start, 0)
}

func (m *memo) hash() uint64 {
	return mix(m.front ^ mix(m.state))
}

func (m *memo) encode(s *search) []byte {
	b := m.buf[:0]
	for _, n := range s.f {
		b = binary.AppendUvarint(b, uint64(n))
	}
	for _, k := range m.keys {
		val := s.value(k)
		b = binary.AppendUvarint(b, uint64(len(val)))
		for _, u := range val {
			b = binary.AppendVarint(b, u)
		}
	}
	m.buf = b
	return b
}

func (m *memo) contains(s *search) bool {
	if withoutMemo {
		return false
	}
	e, ok := m.heads[m.hash()]
	if !ok {
		return false
	}
	cur := m.encode(s)
	for ; e >= 0; e = m.next[e] {
		if bytes.Equal(m.data[m.start[e]:m.start[e+1]], cur) {
			return true
		}
	}
	return false
}

func (m *memo) add(s *search) {
	h := m.hash()
	e := int32(len(m.next))
	prev, ok := m.heads[h]
	if !ok {
		prev = -1
	}
	m.next = append(m.next, prev)
	m.data = append(m.data, m.encode(s)...)
	m.start = append(m.start, int32(len(m.data)))
	m.heads[h] = e
}
