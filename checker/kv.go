package checker

import (
	"encoding/json"
	"fmt"
	"maps"
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

// KVInput represents an input to a key-value store operation.
type KVInput struct {
	Op  string
	Key string
	Val Value
}

// KVModel returns a porcupine.Model for a key-value store.
func KVModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} { return map[string]Value{} },

		Step: func(state, input, output interface{}) (bool, interface{}) {
			q := maps.Clone(state.(map[string]Value))
			in := input.(KVInput)
			outStr, _ := output.(string)
			outVal := ParseValue(outStr)

			switch strings.ToUpper(in.Op) {
			case "PUT":
				// Wrap value in Option to match history.ml VOption usage
				q[in.Key] = in.Val.ToOption()
				return true, q

			case "GET":
				v, ok := q[in.Key]
				if !ok {
					// Expect None (NotFound)
					return outVal.String() == NotFound.String(), q
				}
				// Compare strings of the structured values
				return outVal.String() == v.String(), q

			case "DELETE":
				delete(q, in.Key)
				return true, q
			default:
				return false, state
			}
		},

		Equal: func(a, b interface{}) bool {
			ma := a.(map[string]Value)
			mb := b.(map[string]Value)
			if len(ma) != len(mb) {
				return false
			}
			for k, v := range ma {
				if v2, ok := mb[k]; !ok || v.String() != v2.String() {
					return false
				}
			}
			return true
		},

		DescribeOperation: func(input, output interface{}) string {
			in := input.(KVInput)
			outStr, _ := output.(string)
			outVal := ParseValue(outStr)

			switch strings.ToUpper(in.Op) {
			case "PUT":
				return fmt.Sprintf("PUT '%s' -> %s", in.Key, in.Val.String())
			case "GET":
				return fmt.Sprintf("GET '%s' => %s", in.Key, outVal.String())
			case "DELETE":
				return fmt.Sprintf("DELETE '%s'", in.Key)
			default:
				return fmt.Sprintf("%s %s", in.Op, in.Key)
			}
		},

		DescribeState: func(state interface{}) string {
			m := state.(map[string]Value)
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
				fmt.Fprintf(&b, "%s: %s", k, m[k].String())
			}
			b.WriteString("}")
			return b.String()
		},
	}
}
