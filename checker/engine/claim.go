// Package engine checks a key-value history against a consistency claim:
// linearizability, sequential consistency, or a mix in which each operation
// kind is marked linearizable or sequential. The normative rules are in
// checker/testdata/claims/CONTRACT.md; every verdict, triage and witness this
// package reports is a function of the history, the model and the claim.
package engine

import (
	"fmt"
	"sort"
	"strings"
)

// Kind is an operation kind: a write, a read, or a read-modify-write.
type Kind uint8

const (
	Write Kind = iota
	Read
	RMW
)

func (k Kind) String() string {
	switch k {
	case Write:
		return "Write"
	case Read:
		return "Read"
	case RMW:
		return "RMW"
	}
	return fmt.Sprintf("Kind(%d)", uint8(k))
}

// KindSet is a set of operation kinds, one bit per kind.
type KindSet uint8

func KindsOf(ks ...Kind) KindSet {
	var s KindSet
	for _, k := range ks {
		s |= 1 << k
	}
	return s
}

func (s KindSet) Has(k Kind) bool { return s&(1<<k) != 0 }

// Model names the sequential specification operations are checked against.
type Model uint8

const (
	// KV is the append-log store: a write appends its uid to the key's list.
	KV Model = iota
	// KVRMW is the store with blind writes: a write replaces the key's list
	// with its uid alone, and a read-modify-write appends its uid and returns
	// the prior list.
	KVRMW
)

func ParseModel(s string) (Model, error) {
	switch s {
	case "kv":
		return KV, nil
	case "kv_rmw":
		return KVRMW, nil
	}
	return 0, fmt.Errorf("unknown model %q", s)
}

func (m Model) String() string {
	if m == KVRMW {
		return "kv_rmw"
	}
	return "kv"
}

// Declared is the set of kinds the model defines.
func (m Model) Declared() KindSet {
	if m == KVRMW {
		return KindsOf(Write, Read, RMW)
	}
	return KindsOf(Write, Read)
}

// Mark is the guarantee a claim gives one operation kind.
type Mark uint8

const (
	Linearizable Mark = iota
	Sequential
)

func (m Mark) String() string {
	if m == Sequential {
		return "sequential"
	}
	return "linearizable"
}

// Claim gives each kind the model declares a mark. Kinds the model does not
// declare are ignored.
type Claim struct {
	Model Model
	Marks [3]Mark
}

// ParseClaim builds a claim from its JSON object form: the keys Write, Read
// and, under kv_rmw, RMW, each "linearizable" or "sequential".
func ParseClaim(m Model, marks map[string]string) (Claim, error) {
	c := Claim{Model: m}
	seen := 0
	for _, k := range []Kind{Write, Read, RMW} {
		if !m.Declared().Has(k) {
			continue
		}
		v, ok := marks[k.String()]
		if !ok {
			return c, fmt.Errorf("claim has no mark for %s", k)
		}
		switch v {
		case "linearizable":
			c.Marks[k] = Linearizable
		case "sequential":
			c.Marks[k] = Sequential
		default:
			return c, fmt.Errorf("claim marks %s %q", k, v)
		}
		seen++
	}
	if len(marks) != seen {
		return c, fmt.Errorf("claim names a kind the model %s does not declare", m)
	}
	return c, nil
}

// ClaimWith returns the claim whose linearizable kinds are l.
func ClaimWith(m Model, l KindSet) Claim {
	c := Claim{Model: m}
	for _, k := range []Kind{Write, Read, RMW} {
		if !l.Has(k) {
			c.Marks[k] = Sequential
		}
	}
	return c
}

// Linearizable is the set of declared kinds marked linearizable.
func (c Claim) Linearizable() KindSet {
	var s KindSet
	for _, k := range []Kind{Write, Read, RMW} {
		if c.Model.Declared().Has(k) && c.Marks[k] == Linearizable {
			s |= 1 << k
		}
	}
	return s
}

// Name is the claim's display name.
func (c Claim) Name() string {
	d := c.Model.Declared()
	switch l := c.Linearizable(); {
	case l == d:
		return "linearizable"
	case l == 0:
		return "sequential"
	case l == d&^KindsOf(Read):
		return "ordered_sequential"
	}
	return "mixed"
}

// JSON is the claim's object form, keys in sorted order.
func (c Claim) JSON() string {
	var parts []string
	for _, k := range []Kind{Write, Read, RMW} {
		if c.Model.Declared().Has(k) {
			parts = append(parts, fmt.Sprintf("%q:%q", k.String(), c.Marks[k].String()))
		}
	}
	sort.Strings(parts)
	return "{" + strings.Join(parts, ",") + "}"
}

// Ladder is the levels triage walks, strongest first.
func Ladder(m Model) []Claim {
	d := m.Declared()
	return []Claim{ClaimWith(m, d), ClaimWith(m, d&^KindsOf(Read)), ClaimWith(m, 0)}
}

// Relation is one order relation a witness must extend.
type Relation struct {
	// Session orders the operations of one client.
	Session bool
	// Without Session: a before b when a returned before b was invoked, a's
	// kind is in From and b's kind is in To.
	From, To KindSet
}

// Relations is the claim as order relations: session order and real time
// among the linearizable kinds.
func (c Claim) Relations() []Relation {
	l := c.Linearizable()
	rs := []Relation{{Session: true}}
	if l != 0 {
		rs = append(rs, Relation{From: l, To: l})
	}
	return rs
}

// partitioned reports whether real time covers every pair of declared
// kinds, which is when the check runs one key at a time.
func partitioned(m Model, rs []Relation) bool {
	d := m.Declared()
	for _, r := range rs {
		if !r.Session && r.From&d == d && r.To&d == d {
			return true
		}
	}
	return false
}
