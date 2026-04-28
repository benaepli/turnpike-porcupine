package checker

import (
	"encoding/json"
	"testing"

	"github.com/anishathalye/porcupine"
)

// makeRMWLogValue serializes a []RMWLogEntry as the JSON shape that
// ClientInterface.Read would emit: a VList of VTuple(VOption(VInt), VInt).
func makeRMWLogValue(entries []RMWLogEntry) string {
	type rawValue struct {
		Type string          `json:"type"`
		Raw  json.RawMessage `json:"value"`
	}
	tuples := make([]rawValue, len(entries))
	for i, e := range entries {
		var prevRaw json.RawMessage
		if e.PrevUid == nil {
			prevRaw = json.RawMessage("null")
		} else {
			inner, _ := json.Marshal(rawValue{Type: "VInt", Raw: json.RawMessage(intToJSON(*e.PrevUid))})
			prevRaw = inner
		}
		prev := rawValue{Type: "VOption", Raw: prevRaw}
		uid := rawValue{Type: "VInt", Raw: json.RawMessage(intToJSON(e.Uid))}
		pair := []rawValue{prev, uid}
		pairRaw, _ := json.Marshal(pair)
		tuples[i] = rawValue{Type: "VTuple", Raw: pairRaw}
	}
	listRaw, _ := json.Marshal(tuples)
	listValue := rawValue{Type: "VList", Raw: listRaw}
	out, _ := json.Marshal(listValue)
	return string(out)
}

func intToJSON(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}

func intPtr(n int) *int { return &n }

func runRMW(t *testing.T, ops []porcupine.Operation) bool {
	t.Helper()
	res := porcupine.CheckOperations(KVRMWModel(), ops)
	return res
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
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "PUT", Key: "k", Uid: 7}, nil),
		op(3, 4, KVInput{Op: "GET", Key: "k"}, makeRMWLogValue([]RMWLogEntry{{PrevUid: nil, Uid: 7}})),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: blind PUT then GET")
	}
}

func TestKVRMW_RmwOnEmptyKey(t *testing.T) {
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "RMW", Key: "k", Uid: 9}, nil),
		op(3, 4, KVInput{Op: "GET", Key: "k"}, makeRMWLogValue([]RMWLogEntry{{PrevUid: nil, Uid: 9}})),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: RMW on empty key has prev_uid=nil")
	}
}

func TestKVRMW_PutThenRmwThenGet(t *testing.T) {
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "PUT", Key: "k", Uid: 1}, nil),
		op(3, 4, KVInput{Op: "RMW", Key: "k", Uid: 2}, nil),
		op(5, 6, KVInput{Op: "GET", Key: "k"}, makeRMWLogValue([]RMWLogEntry{
			{PrevUid: nil, Uid: 1},
			{PrevUid: intPtr(1), Uid: 2},
		})),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: PUT 1, RMW(prev=1) 2, GET")
	}
}

func TestKVRMW_GetWithWrongPrevUid(t *testing.T) {
	// Protocol claims RMW saw prev=99 but the only prior entry was uid=1.
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "PUT", Key: "k", Uid: 1}, nil),
		op(3, 4, KVInput{Op: "RMW", Key: "k", Uid: 2}, nil),
		op(5, 6, KVInput{Op: "GET", Key: "k"}, makeRMWLogValue([]RMWLogEntry{
			{PrevUid: nil, Uid: 1},
			{PrevUid: intPtr(99), Uid: 2}, // wrong prev_uid
		})),
	}
	if runRMW(t, ops) {
		t.Fatalf("expected non-linearizable: RMW recorded wrong prev_uid")
	}
}

func TestKVRMW_BlindRmwLooksLikeBlindPut(t *testing.T) {
	// If protocol recorded RMW with prev=nil but model would derive prev=1,
	// the GET must reject.
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "PUT", Key: "k", Uid: 1}, nil),
		op(3, 4, KVInput{Op: "RMW", Key: "k", Uid: 2}, nil),
		op(5, 6, KVInput{Op: "GET", Key: "k"}, makeRMWLogValue([]RMWLogEntry{
			{PrevUid: nil, Uid: 1},
			{PrevUid: nil, Uid: 2}, // wrong: should be Some(1)
		})),
	}
	if runRMW(t, ops) {
		t.Fatalf("expected non-linearizable: RMW dropped prev_uid entirely")
	}
}

func TestKVRMW_ConcurrentRmws(t *testing.T) {
	// Two concurrent RMWs on an empty key. One ordering succeeds: whichever
	// went first sees prev=nil, the other sees Some(first_uid).
	ops := []porcupine.Operation{
		op(1, 4, KVInput{Op: "RMW", Key: "k", Uid: 10}, nil),
		op(2, 5, KVInput{Op: "RMW", Key: "k", Uid: 20}, nil),
		op(6, 7, KVInput{Op: "GET", Key: "k"}, makeRMWLogValue([]RMWLogEntry{
			{PrevUid: nil, Uid: 10},
			{PrevUid: intPtr(10), Uid: 20},
		})),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: ordering 10 then 20 is valid")
	}
}

func TestKVRMW_GetEmptyKey(t *testing.T) {
	ops := []porcupine.Operation{
		op(1, 2, KVInput{Op: "GET", Key: "k"}, makeRMWLogValue(nil)),
	}
	if !runRMW(t, ops) {
		t.Fatalf("expected linearizable: GET on empty key returns []")
	}
}

func TestKVRMW_PendingWriteSyntheticResponse(t *testing.T) {
	// Simulate the BuildOperationsWithAnnotations pending-RMW path by
	// constructing an EventRow stream where an RMW has no Response.
	rows := []*EventRow{
		{UniqueID: "1", ClientID: "0", Kind: "Invocation", Action: Rmw,
			Payload: `["{\"type\":\"VNode\",\"value\":{\"role\":0,\"index\":0}}","{\"type\":\"VString\",\"value\":\"k\"}","{\"type\":\"VInt\",\"value\":42}"]`},
		// No Response row — pending RMW.
		{UniqueID: "2", ClientID: "0", Kind: "Invocation", Action: Read,
			Payload: `["{\"type\":\"VNode\",\"value\":{\"role\":0,\"index\":0}}","{\"type\":\"VString\",\"value\":\"k\"}"]`},
		{UniqueID: "2", ClientID: "0", Kind: "Response", Action: Read,
			Payload: `["` + escapeJSON(makeRMWLogValue([]RMWLogEntry{{PrevUid: nil, Uid: 42}})) + `"]`},
	}
	ops, _ := BuildOperationsWithAnnotations(rows)
	// Should have 2 ops: the synthetic RMW completion and the real GET.
	if len(ops) != 2 {
		t.Fatalf("expected 2 ops (synthetic RMW + GET), got %d", len(ops))
	}
	// Verify the synthetic RMW carries Op="RMW".
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
		t.Fatalf("expected linearizable: RMW(42) followed by GET observing it")
	}
}

// escapeJSON escapes a string for embedding inside a JSON string literal.
func escapeJSON(s string) string {
	b, _ := json.Marshal(s)
	// Strip surrounding quotes added by Marshal.
	return string(b[1 : len(b)-1])
}
