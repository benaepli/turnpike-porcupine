package checker

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/anishathalye/porcupine"
)

type Value struct {
	Type string          `json:"type"`
	Raw  json.RawMessage `json:"value"`
}

var NotFound = Value{Type: "VOption", Raw: []byte("null")}

// ParseValue converts a JSON string into a Value struct.
func ParseValue(s string) Value {
	var v Value
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return Value{Type: "Error", Raw: []byte(fmt.Sprintf("%q", err.Error()))}
	}
	return v
}

// String provides a pretty-printed representation for HTML visualization.
func (v Value) String() string {
	if v.Type == "" {
		return "⊥"
	}

	switch v.Type {
	case "VString":
		var s string
		_ = json.Unmarshal(v.Raw, &s)
		return fmt.Sprintf("%q", s)
	case "VInt":
		var i int
		_ = json.Unmarshal(v.Raw, &i)
		return fmt.Sprintf("%d", i)
	case "VBool":
		var b bool
		_ = json.Unmarshal(v.Raw, &b)
		return fmt.Sprintf("%v", b)
	case "VOption":
		if string(v.Raw) == "null" {
			return "None"
		}
		var inner Value
		_ = json.Unmarshal(v.Raw, &inner)
		return fmt.Sprintf("Some(%s)", inner.String())
	case "VList", "VTuple":
		var list []Value
		_ = json.Unmarshal(v.Raw, &list)
		strs := make([]string, len(list))
		for i, item := range list {
			strs[i] = item.String()
		}
		if v.Type == "VTuple" {
			return fmt.Sprintf("(%s)", strings.Join(strs, ", "))
		}
		return fmt.Sprintf("[%s]", strings.Join(strs, ", "))
	case "VMap":
		var pairs [][]Value
		_ = json.Unmarshal(v.Raw, &pairs)
		strs := make([]string, len(pairs))
		for i, pair := range pairs {
			if len(pair) == 2 {
				strs[i] = fmt.Sprintf("%s -> %s", pair[0].String(), pair[1].String())
			}
		}
		return fmt.Sprintf("{%s}", strings.Join(strs, ", "))
	default:
		return fmt.Sprintf("%s<%s>", v.Type, string(v.Raw))
	}
}

// ToOption wraps a value in a VOption (Some(v)).
func (v Value) ToOption() Value {
	raw, _ := json.Marshal(v)
	return Value{
		Type: "VOption",
		Raw:  raw,
	}
}

// parseVInt extracts an int from a VInt Value. Returns (n, true) on success.
func parseVInt(v Value) (int, bool) {
	if v.Type != "VInt" {
		return 0, false
	}
	var n int
	if err := json.Unmarshal(v.Raw, &n); err != nil {
		return 0, false
	}
	return n, true
}

// KVInput represents an input to a key-value append-log operation.
// For PUT: Uid is the unique write identifier, appended to the per-key log.
// For GET: Uid is unused.
type KVInput struct {
	Op  string
	Key string
	Uid int
}

// parseUidList extracts a []int from a VList-of-VInt Value (a Read response payload).
// Accepts empty string, an "absent" VOption null, or a VList. Returns nil slice on parse failure.
func parseUidList(v Value) ([]int, bool) {
	switch v.Type {
	case "":
		return nil, true
	case "VOption":
		if string(v.Raw) == "null" {
			return nil, true
		}
		var inner Value
		if err := json.Unmarshal(v.Raw, &inner); err != nil {
			return nil, false
		}
		return parseUidList(inner)
	case "VList":
		var items []Value
		if err := json.Unmarshal(v.Raw, &items); err != nil {
			return nil, false
		}
		out := make([]int, len(items))
		for i, it := range items {
			if it.Type != "VInt" {
				return nil, false
			}
			var n int
			if err := json.Unmarshal(it.Raw, &n); err != nil {
				return nil, false
			}
			out[i] = n
		}
		return out, true
	}
	return nil, false
}

func uidListEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func formatUidList(xs []int) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = fmt.Sprintf("%d", x)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func cloneLogs(m map[string][]int) map[string][]int {
	out := make(map[string][]int, len(m))
	for k, v := range m {
		cp := make([]int, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

// KVModel returns a porcupine.Model for an append-log key-value store.
// State: map[key] -> ordered list of uids committed for that key.
// PUT(key, uid): appends uid to state[key].
// GET(key) -> []uid: must equal state[key] at some linearization point.
func KVModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} { return map[string][]int{} },

		Step: func(state, input, output interface{}) (bool, interface{}) {
			q := cloneLogs(state.(map[string][]int))
			in := input.(KVInput)

			switch strings.ToUpper(in.Op) {
			case "PUT":
				q[in.Key] = append(q[in.Key], in.Uid)
				return true, q

			case "GET":
				outStr, _ := output.(string)
				outVal := ParseValue(outStr)
				observed, ok := parseUidList(outVal)
				if !ok {
					return false, q
				}
				return uidListEqual(observed, q[in.Key]), q

			default:
				return false, state
			}
		},

		Equal: func(a, b interface{}) bool {
			ma := a.(map[string][]int)
			mb := b.(map[string][]int)
			if len(ma) != len(mb) {
				return false
			}
			for k, v := range ma {
				v2, ok := mb[k]
				if !ok || !uidListEqual(v, v2) {
					return false
				}
			}
			return true
		},

		DescribeOperation: func(input, output interface{}) string {
			in := input.(KVInput)
			switch strings.ToUpper(in.Op) {
			case "PUT":
				return fmt.Sprintf("PUT '%s' <- %d", in.Key, in.Uid)
			case "GET":
				outStr, _ := output.(string)
				outVal := ParseValue(outStr)
				if list, ok := parseUidList(outVal); ok {
					return fmt.Sprintf("GET '%s' => %s", in.Key, formatUidList(list))
				}
				return fmt.Sprintf("GET '%s' => %s", in.Key, outVal.String())
			default:
				return fmt.Sprintf("%s %s", in.Op, in.Key)
			}
		},

		DescribeState: func(state interface{}) string {
			m := state.(map[string][]int)
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var b strings.Builder
			b.WriteString("{")
			for i, k := range keys {
				if i > 0 {
					b.WriteString(", ")
				}
				fmt.Fprintf(&b, "%s: %s", k, formatUidList(m[k]))
			}
			b.WriteString("}")
			return b.String()
		},
	}
}
