package checker

import (
	"encoding/json"
	"testing"

	"github.com/anishathalye/porcupine"
)

// makeUidListValue serializes a []int as the JSON shape that
// Client.Read or Client.RMW would emit: a VList of VInt.
func makeUidListValue(uids []int) string {
	type rawValue struct {
		Type string          `json:"type"`
		Raw  json.RawMessage `json:"value"`
	}
	items := make([]rawValue, len(uids))
	for i, u := range uids {
		items[i] = rawValue{Type: "VInt", Raw: json.RawMessage(intToJSON(u))}
	}
	listRaw, _ := json.Marshal(items)
	out, _ := json.Marshal(rawValue{Type: "VList", Raw: listRaw})
	return string(out)
}

func intToJSON(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

func runRMW(t *testing.T, ops []porcupine.Operation) bool {
	t.Helper()
	return porcupine.CheckOperations(KVRMWModel(), ops)
}

func op(call, ret int64, in KVInput, out interface{}) porcupine.Operation {
	return porcupine.Operation{
		Input:    in,
		Output:   out,
		Call:     call,
		Return:   ret,
		ClientId: int(call),
	}
}

func TestKVRMW_PutThenGet(t *testing.T) {
	// Blind PUT overwrites the key with [uid].
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "PUT", Key: "k", Uid: 7}, nil),
		op(3, 4, KVInput{Op: "GET", Key: "k"}, makeUidListValue([]int{7})),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: blind PUT then GET")
	}
}

func TestKVRMW_PutOverwritesPriorState(t *testing.T) {
	// Two blind PUTs: the second overwrites the first; GET sees only the second.
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "PUT", Key: "k", Uid: 1}, nil),
		op(3, 4, KVInput{Op: "PUT", Key: "k", Uid: 2}, nil),
		op(5, 6, KVInput{Op: "GET", Key: "k"}, makeUidListValue([]int{2})),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: PUT 2 overwrites PUT 1, GET sees [2]")
	}
}

func TestKVRMW_PutOverwriteRejectsAppendObservation(t *testing.T) {
	// If GET observes [1, 2] after two blind PUTs, the model must reject —
	// PUT is overwrite, not append.
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "PUT", Key: "k", Uid: 1}, nil),
		op(3, 4, KVInput{Op: "PUT", Key: "k", Uid: 2}, nil),
		op(5, 6, KVInput{Op: "GET", Key: "k"}, makeUidListValue([]int{1, 2})),
	}
	if runRMW(t, ops) {
		t.Fatalf("expected non-linearizable: GET cannot see appended log under blind PUT semantics")
	}
}

func TestKVRMW_RmwOnEmptyKeyReturnsEmpty(t *testing.T) {
	// RMW on an empty key returns [], appends uid; GET sees [uid].
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "RMW", Key: "k", Uid: 9}, makeUidListValue(nil)),
		op(3, 4, KVInput{Op: "GET", Key: "k"}, makeUidListValue([]int{9})),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: RMW on empty key returns []")
	}
}

func TestKVRMW_RmwWrongOldOutput(t *testing.T) {
	// RMW claims to have observed [99] but the key was empty.
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "RMW", Key: "k", Uid: 9}, makeUidListValue([]int{99})),
	}
	if runRMW(t, ops) {
		t.Fatalf("expected non-linearizable: RMW returned wrong old value")
	}
}

func TestKVRMW_PutThenRmwThenGet(t *testing.T) {
	// PUT 1, RMW 2 (must return [1]), GET sees [1, 2].
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "PUT", Key: "k", Uid: 1}, nil),
		op(3, 4, KVInput{Op: "RMW", Key: "k", Uid: 2}, makeUidListValue([]int{1})),
		op(5, 6, KVInput{Op: "GET", Key: "k"}, makeUidListValue([]int{1, 2})),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: PUT 1, RMW returning [1], GET returning [1, 2]")
	}
}

func TestKVRMW_RmwAfterPutWithWrongOld(t *testing.T) {
	// RMW after PUT 1 must return [1], not [].
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "PUT", Key: "k", Uid: 1}, nil),
		op(3, 4, KVInput{Op: "RMW", Key: "k", Uid: 2}, makeUidListValue(nil)),
	}
	if runRMW(t, ops) {
		t.Fatalf("expected non-linearizable: RMW after PUT must return [1]")
	}
}

func TestKVRMW_ConcurrentRmws(t *testing.T) {
	// Two concurrent RMWs on an empty key. Valid ordering: 10 first (returns
	// []), then 20 (returns [10]); GET sees [10, 20].
	ops := []porcupine.Operation{
		op(1, 4, KVInput{Op: "RMW", Key: "k", Uid: 10}, makeUidListValue(nil)),
		op(2, 5, KVInput{Op: "RMW", Key: "k", Uid: 20}, makeUidListValue([]int{10})),
		op(6, 7, KVInput{Op: "GET", Key: "k"}, makeUidListValue([]int{10, 20})),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: ordering 10 then 20")
	}
}

func TestKVRMW_GetEmptyKey(t *testing.T) {
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "GET", Key: "k"}, makeUidListValue(nil)),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: GET on empty key returns []")
	}
}

func TestKVRMW_PendingRmwSyntheticResponse(t *testing.T) {
	// A pending (no-response) RMW gets synthesized with Output=nil. The model
	// must accept it (skip the output check) so the run can still linearize.
	rows := []*EventRow{
		{UniqueID: "1", ClientID: "0", Kind: "Invocation", Action: Rmw,
			Payload: `["{\"type\":\"VNode\",\"value\":{\"role\":0,\"index\":0}}","{\"type\":\"VString\",\"value\":\"k\"}","{\"type\":\"VInt\",\"value\":42}"]`},
		{UniqueID: "2", ClientID: "0", Kind: "Invocation", Action: Read,
			Payload: `["{\"type\":\"VNode\",\"value\":{\"role\":0,\"index\":0}}","{\"type\":\"VString\",\"value\":\"k\"}"]`},
		{UniqueID: "2", ClientID: "0", Kind: "Response", Action: Read,
			Payload: `["` + escapeJSON(makeUidListValue([]int{42})) + `"]`},
	}
	ops, _ := BuildOperationsWithAnnotations(rows)
	if len(ops) != 2 {
		t.Fatalf("expected 2 ops (synthetic RMW + GET), got %d", len(ops))
	}
	foundRMW := false
	for _, o := range ops {
		if in, ok := o.Input.(KVInput); ok && in.Op == "RMW" && in.Uid == 42 {
			foundRMW = true
		}
	}
	if !foundRMW {
		t.Fatalf("expected synthetic RMW op with Uid=42, got ops: %+v", ops)
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: pending RMW accepted, GET observes [42]")
	}
}

// escapeJSON escapes a string for embedding inside a JSON string literal.
func escapeJSON(s string) string {
	b, _ := json.Marshal(s)
	return string(b[1 : len(b)-1])
}

func TestClientActionStrings(t *testing.T) {
	cases := map[string]ActionType{
		"Client.Read":          Read,
		"Client.Write":         Write,
		"Client.RMW":           Rmw,
		"System.Crash":         Crash,
		"System.Recover":       Recover,
		"ClientInterface.Read": "Unknown operation.",
	}
	for in, want := range cases {
		var got ActionType
		if err := got.UnmarshalCSV(in); err != nil || got != want {
			t.Errorf("%q: got %q err %v, want %q", in, got, err, want)
		}
	}
}

func TestUnitDestinationPayload(t *testing.T) {
	// An operation without a destination carries a unit value where the node
	// would be; the key and uid keep their positions.
	unit := `"{\"type\":\"VUnit\",\"value\":null}"`
	key := `"{\"type\":\"VString\",\"value\":\"k\"}"`
	rows := []*EventRow{
		{UniqueID: "1", ClientID: "0", Kind: "Invocation", Action: Write,
			Payload: `[` + unit + `,` + key + `,"{\"type\":\"VInt\",\"value\":5}"]`},
		{UniqueID: "1", ClientID: "0", Kind: "Response", Action: Write,
			Payload: `["{\"type\":\"VUnit\",\"value\":null}"]`},
		{UniqueID: "2", ClientID: "0", Kind: "Invocation", Action: Rmw,
			Payload: `[` + unit + `,` + key + `,"{\"type\":\"VInt\",\"value\":6}"]`},
		{UniqueID: "2", ClientID: "0", Kind: "Response", Action: Rmw,
			Payload: `["` + escapeJSON(makeUidListValue([]int{5})) + `"]`},
		{UniqueID: "3", ClientID: "0", Kind: "Invocation", Action: Read,
			Payload: `[` + unit + `,` + key + `]`},
		{UniqueID: "3", ClientID: "0", Kind: "Response", Action: Read,
			Payload: `["` + escapeJSON(makeUidListValue([]int{5, 6})) + `"]`},
	}
	ops, _ := BuildOperationsWithAnnotations(rows)
	if len(ops) != 3 {
		t.Fatalf("expected 3 ops, got %d: %+v", len(ops), ops)
	}
	// Keys are stored in their printed form, quoted.
	want := []KVInput{{Op: "PUT", Key: `"k"`, Uid: 5}, {Op: "RMW", Key: `"k"`, Uid: 6}, {Op: "GET", Key: `"k"`}}
	for i, o := range ops {
		if o.Input.(KVInput) != want[i] {
			t.Errorf("op %d: got %+v, want %+v", i, o.Input, want[i])
		}
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: PUT 5, RMW 6 returning [5], GET [5, 6]")
	}
}

func TestAnnotationsNameNodesByDeployment(t *testing.T) {
	crash := func(index int) *EventRow {
		return &EventRow{UniqueID: "9", ClientID: "-1", Kind: "Crash", Action: Crash,
			Payload: `["{\"type\":\"VNode\",\"value\":{\"role\":0,\"index\":` + intToJSON(index) + `}}"]`}
	}
	rows := []*EventRow{crash(1), crash(2), crash(3)}
	names := func(index int) (NodeLabel, bool) {
		switch index {
		case 1:
			return NodeLabel{Role: "Node", Ordinal: 0, Path: "shards[1].nodes[0]"}, true
		case 2:
			return NodeLabel{Role: "Router", Ordinal: 0}, true
		}
		return NodeLabel{}, false
	}
	_, ann := BuildOperationsWithNodeNames(rows, names)
	if len(ann) != 3 {
		t.Fatalf("expected 3 annotations, got %d", len(ann))
	}
	want := []struct{ tag, details string }{
		{"Node[0] (node 1)", "Node[0] (node 1) at shards[1].nodes[0] crashed"},
		{"Router[0] (node 2)", "Router[0] (node 2) crashed"},
		{"Node 3", "Node 3 crashed"},
	}
	for i, w := range want {
		if ann[i].Tag != w.tag || ann[i].Details != w.details {
			t.Errorf("annotation %d: got %q / %q, want %q / %q", i, ann[i].Tag, ann[i].Details, w.tag, w.details)
		}
	}
	_, ann = BuildOperationsWithAnnotations(rows[:1])
	if ann[0].Tag != "Node 1" {
		t.Errorf("without names: got tag %q, want Node 1", ann[0].Tag)
	}
}
