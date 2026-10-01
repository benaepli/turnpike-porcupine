package engine

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// RowKind is the kind of a history row.
type RowKind uint8

const (
	Invocation RowKind = iota
	Response
	// System is a row that is not a client operation, such as a crash or a
	// recovery; the check ignores it.
	System
)

// Row is one history row. Key, UID and Value are meaningful only where the
// matching Has flag is set; a converter that could not read a field it found
// sets Malformed instead.
type Row struct {
	Kind     RowKind
	ID       int64
	Client   int64
	Action   string
	Key      string
	HasKey   bool
	UID      int64
	HasUID   bool
	Value    []int64
	HasValue bool
	// Malformed is non-empty when a field was present but unreadable.
	Malformed string
}

// ActionKind returns the operation kind an action names. The action may
// carry a prefix; the kind is taken from the suffix.
func ActionKind(action string) (Kind, bool) {
	switch {
	case strings.HasSuffix(action, "Client.Write"):
		return Write, true
	case strings.HasSuffix(action, "Client.Read"):
		return Read, true
	case strings.HasSuffix(action, "Client.RMW"):
		return RMW, true
	}
	return 0, false
}

// RowFromEvent converts an executions-table row: kind is "Invocation",
// "Response" or a system kind, and payload is the row's JSON array of
// values. An invocation's payload is [destination, key, uid] for a write or
// read-modify-write and [destination, key] for a read; a read or
// read-modify-write response's payload holds the list.
func RowFromEvent(uniqueID, clientID, kind, action, payload string) Row {
	var r Row
	switch kind {
	case "Invocation":
		r.Kind = Invocation
	case "Response":
		r.Kind = Response
	default:
		r.Kind = System
		r.Action = action
		return r
	}
	r.Action = action
	var err error
	if r.ID, err = strconv.ParseInt(strings.TrimSpace(uniqueID), 10, 64); err != nil {
		r.Malformed = fmt.Sprintf("unique id %q", uniqueID)
		return r
	}
	if r.Client, err = strconv.ParseInt(strings.TrimSpace(clientID), 10, 64); err != nil {
		r.Malformed = fmt.Sprintf("client id %q", clientID)
		return r
	}
	k, ok := ActionKind(action)
	if !ok {
		return r
	}
	if scanPayload(&r, k, payload) {
		return r
	}
	return decodePayload(r, k, payload)
}

// decodePayload is RowFromEvent's general decoder, for payloads outside the
// scanner's shape.
func decodePayload(r Row, k Kind, payload string) Row {
	items, err := payloadItems(payload)
	if err != nil {
		r.Malformed = err.Error()
		return r
	}
	if r.Kind == Invocation {
		if len(items) >= 2 {
			s, ok := stringValue(items[1])
			if !ok {
				r.Malformed = fmt.Sprintf("key %s", items[1])
				return r
			}
			r.Key, r.HasKey = s, true
		}
		if k != Read && len(items) >= 3 {
			n, ok := intValue(items[2])
			if !ok {
				r.Malformed = fmt.Sprintf("uid %s", items[2])
				return r
			}
			r.UID, r.HasUID = n, true
		}
		return r
	}
	if k != Write && len(items) >= 1 {
		l, ok := listValue(items[0])
		if !ok {
			r.Malformed = fmt.Sprintf("value %s", items[0])
			return r
		}
		r.Value, r.HasValue = l, true
	}
	return r
}

type rawValue struct {
	Type  string          `json:"type"`
	Value json.RawMessage `json:"value"`
}

// payloadItems splits a payload array; an item that is a JSON string holds
// the value's JSON text.
func payloadItems(payload string) ([]json.RawMessage, error) {
	if strings.TrimSpace(payload) == "" {
		return nil, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal([]byte(payload), &items); err != nil {
		return nil, fmt.Errorf("payload %q: %v", payload, err)
	}
	for i, it := range items {
		var s string
		if json.Unmarshal(it, &s) == nil {
			items[i] = json.RawMessage(s)
		}
	}
	return items, nil
}

func decodeValue(raw json.RawMessage) (rawValue, bool) {
	var v rawValue
	if json.Unmarshal(raw, &v) != nil || v.Type == "" {
		return v, false
	}
	return v, true
}

func stringValue(raw json.RawMessage) (string, bool) {
	v, ok := decodeValue(raw)
	if !ok || v.Type != "VString" {
		return "", false
	}
	var s string
	if json.Unmarshal(v.Value, &s) != nil {
		return "", false
	}
	return s, true
}

func intValue(raw json.RawMessage) (int64, bool) {
	v, ok := decodeValue(raw)
	if !ok || v.Type != "VInt" {
		return 0, false
	}
	var n int64
	if json.Unmarshal(v.Value, &n) != nil {
		return 0, false
	}
	return n, true
}

func listValue(raw json.RawMessage) ([]int64, bool) {
	v, ok := decodeValue(raw)
	if !ok {
		return nil, false
	}
	switch v.Type {
	case "VOption":
		if strings.TrimSpace(string(v.Value)) == "null" {
			return nil, false
		}
		return listValue(v.Value)
	case "VList":
		var items []json.RawMessage
		if json.Unmarshal(v.Value, &items) != nil {
			return nil, false
		}
		out := make([]int64, len(items))
		for i, it := range items {
			n, ok := intValue(it)
			if !ok {
				return nil, false
			}
			out[i] = n
		}
		return out, true
	}
	return nil, false
}

// op is one client operation. Operations are stored in call-time order, so
// an operation's index orders calls.
type op struct {
	id        int64
	client    int64
	kind      Kind
	key       int32
	uid       int64
	obsStart  int32
	obsEnd    int32
	completed bool
	call, ret int32
}

// History is a history converted and prepared for checking under one model.
// It is immutable once built and may be checked under any number of claims.
type History struct {
	model      Model
	adapterErr string

	ops    []op
	arena  []int64
	keys   []string
	byKey  [][]int32 // per key, its operations in call order
	keyOrd []int32   // key ids in byte order of the key
	uidOp  map[int64]int32

	clients []int64 // distinct client ids, ascending

	value *ValueFailure

	inferred []keyEdges // per key
}

func (h *History) obs(o int32) []int64 {
	p := &h.ops[o]
	return h.arena[p.obsStart:p.obsEnd]
}

// Prepare converts rows and runs the adapter checks, the value checks and
// the edge inference, which depend on the model but not on the claim.
func Prepare(rows []Row, m Model) *History {
	h := &History{model: m}
	if err := h.convert(rows); err != "" {
		h.adapterErr = err
		h.ops = nil
		return h
	}
	h.valueChecks()
	if h.value == nil {
		h.infer()
	}
	return h
}

func (h *History) convert(rows []Row) string {
	type pending struct {
		row  int
		call int32
		ret  int32
		obs  []int64
		done bool
	}
	open := make(map[int64]int, len(rows)/2)
	var calls []pending
	var invRows []*Row
	t := int32(0)
	for i := range rows {
		r := &rows[i]
		if r.Kind == System {
			continue
		}
		t++
		k, ok := ActionKind(r.Action)
		if !ok {
			return fmt.Sprintf("action %q is not a client operation", r.Action)
		}
		if k == RMW && h.model == KV {
			return fmt.Sprintf("operation %d is a read-modify-write under model kv", r.ID)
		}
		if r.Malformed != "" {
			return fmt.Sprintf("operation %d: malformed %s", r.ID, r.Malformed)
		}
		if r.Kind == Invocation {
			if _, dup := open[r.ID]; dup {
				return fmt.Sprintf("two invocations of operation %d", r.ID)
			}
			if !r.HasKey {
				return fmt.Sprintf("operation %d has no key", r.ID)
			}
			if k != Read && !r.HasUID {
				return fmt.Sprintf("operation %d has no uid", r.ID)
			}
			open[r.ID] = len(calls)
			calls = append(calls, pending{row: i, call: t})
			invRows = append(invRows, r)
			continue
		}
		j, ok := open[r.ID]
		if !ok || calls[j].done {
			return fmt.Sprintf("response of operation %d has no open invocation", r.ID)
		}
		inv := invRows[j]
		if inv.Client != r.Client || inv.Action != r.Action {
			return fmt.Sprintf("response of operation %d differs from its invocation", r.ID)
		}
		if k != Write {
			if !r.HasValue {
				return fmt.Sprintf("response of operation %d has no value", r.ID)
			}
			calls[j].obs = r.Value
		}
		calls[j].ret = t
		calls[j].done = true
	}
	end := t + 1

	uids := make(map[int64]int64)
	for j, c := range calls {
		r := invRows[j]
		k, _ := ActionKind(r.Action)
		if k != Read {
			if other, dup := uids[r.UID]; dup {
				return fmt.Sprintf("operations %d and %d share uid %d", other, r.ID, r.UID)
			}
			uids[r.UID] = r.ID
		}
		if !c.done {
			calls[j].ret = end
		}
	}

	bySession := make(map[int64][]int)
	for j := range calls {
		c := invRows[j].Client
		bySession[c] = append(bySession[c], j)
	}
	for _, js := range bySession {
		for x := 1; x < len(js); x++ {
			a, b := calls[js[x-1]], calls[js[x]]
			if b.call < a.ret {
				return fmt.Sprintf("operation %d of client %d was invoked before operation %d returned",
					invRows[js[x]].ID, invRows[js[x]].Client, invRows[js[x-1]].ID)
			}
		}
	}

	keyID := make(map[string]int32)
	h.uidOp = make(map[int64]int32, len(uids))
	clients := make(map[int64]bool)
	for j, c := range calls {
		r := invRows[j]
		k, _ := ActionKind(r.Action)
		if k == Read && !c.done {
			continue
		}
		kid, ok := keyID[r.Key]
		if !ok {
			kid = int32(len(h.keys))
			keyID[r.Key] = kid
			h.keys = append(h.keys, r.Key)
			h.byKey = append(h.byKey, nil)
		}
		o := op{id: r.ID, client: r.Client, kind: k, key: kid, uid: r.UID,
			completed: c.done, call: c.call, ret: c.ret}
		if k == Read {
			o.uid = 0
		}
		o.obsStart = int32(len(h.arena))
		h.arena = append(h.arena, c.obs...)
		o.obsEnd = int32(len(h.arena))
		idx := int32(len(h.ops))
		h.ops = append(h.ops, o)
		h.byKey[kid] = append(h.byKey[kid], idx)
		if k != Read {
			h.uidOp[o.uid] = idx
		}
		clients[r.Client] = true
	}
	for c := range clients {
		h.clients = append(h.clients, c)
	}
	sort.Slice(h.clients, func(a, b int) bool { return h.clients[a] < h.clients[b] })
	h.keyOrd = make([]int32, len(h.keys))
	for i := range h.keyOrd {
		h.keyOrd[i] = int32(i)
	}
	sort.Slice(h.keyOrd, func(a, b int) bool { return h.keys[h.keyOrd[a]] < h.keys[h.keyOrd[b]] })
	return ""
}
