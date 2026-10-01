package checker

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"

	"github.com/anishathalye/porcupine"
)

// The upstream porcupine models and history conversion below exist only so
// the tests can check the engine against upstream porcupine as an oracle.

// ActionType represents the type of action performed on the data structure
type ActionType string

const (
	Read    ActionType = "read"
	Write   ActionType = "write"
	Rmw     ActionType = "rmw"
	Delete  ActionType = "delete"
	Crash   ActionType = "crash"
	Recover ActionType = "recover"
	Timeout ActionType = "timeout"
)

func (e *ActionType) UnmarshalCSV(value string) error {
	*e = ParseAction(value)
	return nil
}

// ParseAction classifies an action string by its suffix.
func ParseAction(value string) ActionType {
	switch {
	case strings.HasSuffix(value, "Client.Read"):
		return Read
	case strings.HasSuffix(value, "Client.Write"):
		return Write
	case strings.HasSuffix(value, "Client.RMW"):
		return Rmw
	case strings.HasSuffix(value, "Client.Delete"):
		return Delete
	case strings.HasSuffix(value, "System.Crash"):
		return Crash
	case strings.HasSuffix(value, "System.Recover"):
		return Recover
	case strings.HasSuffix(value, "Client.SimulateTimeout"):
		return Timeout
	}
	return "Unknown operation."
}

type pendingInvocation struct {
	invRow   *EventRow
	action   ActionType
	callTime int64
	clientID int
}

func mustAtoi(s string) int {
	v, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		log.Fatalf("bad int %q: %v", s, err)
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

// KVRMWModel returns a porcupine.Model for a kv store with three operations:
// blind Write (PUT), append-and-return-old RMW, and Read (GET).
// State: map[key] -> list of uids (same shape as KVModel).
//   - PUT(key, uid): blind overwrite — state[key] = [uid]. No output check.
//   - RMW(key, uid): old = state[key]; state[key] = append(old, uid). Output
//     must equal old. A nil output (pending invocation) skips the check.
//   - GET(key) -> []uid: must equal state[key].
func KVRMWModel() porcupine.Model {
	return porcupine.Model{
		Init: func() interface{} { return map[string][]int{} },

		Step: func(state, input, output interface{}) (bool, interface{}) {
			q := cloneLogs(state.(map[string][]int))
			in := input.(KVInput)

			switch strings.ToUpper(in.Op) {
			case "PUT":
				q[in.Key] = []int{in.Uid}
				return true, q

			case "RMW":
				old := q[in.Key]
				next := make([]int, len(old)+1)
				copy(next, old)
				next[len(old)] = in.Uid
				q[in.Key] = next
				if output == nil {
					return true, q
				}
				outStr, _ := output.(string)
				observed, ok := parseUidList(ParseValue(outStr))
				if !ok {
					return false, q
				}
				return uidListEqual(observed, old), q

			case "GET":
				outStr, _ := output.(string)
				observed, ok := parseUidList(ParseValue(outStr))
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
			case "RMW":
				outStr, _ := output.(string)
				outVal := ParseValue(outStr)
				if list, ok := parseUidList(outVal); ok {
					return fmt.Sprintf("RMW '%s' <- %d (old %s)", in.Key, in.Uid, formatUidList(list))
				}
				return fmt.Sprintf("RMW '%s' <- %d (old %s)", in.Key, in.Uid, outVal.String())
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

// BuildOperations converts a slice of EventRows into porcupine Operations.
func BuildOperations(eventRows []*EventRow) []porcupine.Operation {
	ops, _ := BuildOperationsWithAnnotations(eventRows)
	return ops
}

// BuildOperationsWithAnnotations converts a slice of EventRows into porcupine Operations
// and also returns annotations for system events (Crash, Recover, Timeout) to overlay
// on the visualization. Nodes are named by global index.
func BuildOperationsWithAnnotations(eventRows []*EventRow) ([]porcupine.Operation, []porcupine.Annotation) {
	return BuildOperationsWithNodeNames(eventRows, nil)
}

// BuildOperationsWithNodeNames is BuildOperationsWithAnnotations with
// annotations naming each node through names. A nil names, or a node names
// does not know, is named by global index alone.
func BuildOperationsWithNodeNames(eventRows []*EventRow, names NodeNames) ([]porcupine.Operation, []porcupine.Annotation) {
	var ops []porcupine.Operation
	var annotations []porcupine.Annotation
	pendingInvocations := make(map[string]pendingInvocation)

	for i, row := range eventRows {
		syntheticTime := int64(i + 1)
		action := ParseAction(row.Action)

		switch action {
		case Crash:
			tag, who := nodeText(names, extractNodeID(row.Payload))
			annotations = append(annotations, porcupine.Annotation{
				Tag:             tag,
				Start:           syntheticTime,
				Description:     "💥 Crash",
				Details:         who + " crashed",
				BackgroundColor: "#ff6b6b",
				TextColor:       "#ffffff",
			})
			continue
		case Recover:
			tag, who := nodeText(names, extractNodeID(row.Payload))
			annotations = append(annotations, porcupine.Annotation{
				Tag:             tag,
				Start:           syntheticTime,
				Description:     "🔄 Recover",
				Details:         who + " recovered",
				BackgroundColor: "#51cf66",
				TextColor:       "#ffffff",
			})
			continue
		}

		if row.Kind == "Invocation" {
			if _, exists := pendingInvocations[row.UniqueID]; exists {
				log.Printf("Warning: Found duplicate invocation for UniqueID %s. Overwriting.", row.UniqueID)
			}
			clientID := mustAtoi(row.ClientID)
			pendingInvocations[row.UniqueID] = pendingInvocation{
				invRow:   row,
				action:   action,
				callTime: syntheticTime,
				clientID: clientID,
			}
			// Handle system events as annotations
			switch action {
			case Timeout:
				tag, who := nodeText(names, extractNodeID(row.Payload))
				annotations = append(annotations, porcupine.Annotation{
					Tag:             tag,
					Start:           syntheticTime,
					Description:     "⏱️ Timeout",
					Details:         who + " simulated timeout",
					BackgroundColor: "#fcc419",
					TextColor:       "#000000",
				})
				continue
			}

		} else if row.Kind == "Response" {
			inv, ok := pendingInvocations[row.UniqueID]
			if !ok {
				log.Printf("Warning: Found response for UniqueID %s without matching invocation. Skipping.", row.UniqueID)
				continue
			}
			delete(pendingInvocations, row.UniqueID)

			retTime := syntheticTime
			invRow := inv.invRow
			respRow := row

			// Skip unknown/other system events for linearizability checking
			if inv.action != Read && inv.action != Write && inv.action != Rmw {
				continue
			}

			// Parse payload arrays from both invocation and response
			invPayloads := parsePayloadArray(invRow.Payload)
			respPayloads := parsePayloadArray(respRow.Payload)

			var opInput interface{}
			var opOutput interface{}

			switch inv.action {
			case Write:
				// Write: Payload[0]=node or unit, Payload[1]=key, Payload[2]=uid (VInt)
				if len(invPayloads) < 3 {
					log.Printf("Warning: Write invocation for UniqueID %s has insufficient payloads. Skipping.", row.UniqueID)
					continue
				}
				keyVal := ParseValue(invPayloads[1])
				uidVal := ParseValue(invPayloads[2])
				uid, ok := parseVInt(uidVal)
				if !ok {
					log.Printf("Warning: Write invocation for UniqueID %s has non-int uid payload. Skipping.", row.UniqueID)
					continue
				}
				opInput = KVInput{
					Op:  "PUT",
					Key: keyVal.String(),
					Uid: uid,
				}
				if len(respPayloads) > 0 {
					opOutput = respPayloads[0]
				}
			case Rmw:
				// RMW: same payload shape as Write - Payload[0]=node or unit, Payload[1]=key, Payload[2]=uid.
				if len(invPayloads) < 3 {
					log.Printf("Warning: RMW invocation for UniqueID %s has insufficient payloads. Skipping.", row.UniqueID)
					continue
				}
				keyVal := ParseValue(invPayloads[1])
				uidVal := ParseValue(invPayloads[2])
				uid, ok := parseVInt(uidVal)
				if !ok {
					log.Printf("Warning: RMW invocation for UniqueID %s has non-int uid payload. Skipping.", row.UniqueID)
					continue
				}
				opInput = KVInput{
					Op:  "RMW",
					Key: keyVal.String(),
					Uid: uid,
				}
				if len(respPayloads) > 0 {
					opOutput = respPayloads[0]
				}
			case Read:
				// Read: Payload[0]=node or unit, Payload[1]=key
				if len(invPayloads) < 2 {
					log.Printf("Warning: Read invocation for UniqueID %s has insufficient payloads. Skipping.", row.UniqueID)
					continue
				}
				keyVal := ParseValue(invPayloads[1])
				opInput = KVInput{
					Op:  "GET",
					Key: keyVal.String(),
				}
				if len(respPayloads) > 0 {
					opOutput = respPayloads[0]
				}
			}
			ops = append(ops, porcupine.Operation{
				Input:    opInput,
				Output:   opOutput,
				Call:     inv.callTime,
				Return:   retTime,
				ClientId: inv.clientID,
			})
		}
	}

	// Handle pending Write/RMW invocations by creating synthetic responses at the end.
	// Both are write-like and void from the linearization model's perspective.
	finalTime := int64(len(eventRows) + 1)
	for _, inv := range pendingInvocations {
		var opName string
		switch inv.action {
		case Write:
			opName = "PUT"
		case Rmw:
			opName = "RMW"
		default:
			continue
		}

		invRow := inv.invRow
		invPayloads := parsePayloadArray(invRow.Payload)

		if len(invPayloads) < 3 {
			log.Printf("Warning: Pending %s invocation for UniqueID %s has insufficient payloads. Skipping.", opName, invRow.UniqueID)
			continue
		}

		keyVal := ParseValue(invPayloads[1])
		uidVal := ParseValue(invPayloads[2])
		uid, ok := parseVInt(uidVal)
		if !ok {
			log.Printf("Warning: Pending %s invocation for UniqueID %s has non-int uid payload. Skipping.", opName, invRow.UniqueID)
			continue
		}
		opInput := KVInput{
			Op:  opName,
			Key: keyVal.String(),
			Uid: uid,
		}

		// Synthetic operation that "completes" at the very end.
		ops = append(ops, porcupine.Operation{
			Input:    opInput,
			Output:   nil, // Output irrelevant for write-like ops in these models.
			Call:     inv.callTime,
			Return:   finalTime,
			ClientId: inv.clientID,
		})
	}

	return ops, annotations
}
