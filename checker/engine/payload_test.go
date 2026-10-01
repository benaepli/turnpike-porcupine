package engine

import (
	"math/rand"
	"slices"
	"strings"
	"testing"
)

// TestScannerMatchesDecoder checks that whenever the scanner accepts a
// payload it reads what the general decoder reads.
func TestScannerMatchesDecoder(t *testing.T) {
	node := []string{`{"type":"VNode","value":{"index":0,"role":9}}`, `{"type":"VUnit","value":null}`, `null`, `{"type":"VTuple","value":[]}`}
	u := func(hex string) string { return "\\" + "u" + hex }
	keys := []string{`{"type":"VString","value":"key1"}`,
		`{"value":"k\"\\` + u("00e9") + u("d83d") + u("de00") + u("2028") + `","type":"VString"}`,
		"{\"type\":\"VString\",\"value\":\"raw\xc3\xa9\xe2\x80\xa8\"}", `{"type":"VString","value":"` + u("00e9") + `"}`,
		`{"type":"VString","value":"bad` + u("d800") + `"}`, `{"type":"VInt","value":3}`, `{"type":"VString","value":"x","extra":1}`,
		`{"type":"VString","Value":"x"}`, `{"type":"VString","value":"x","value":"y"}`, `"{\"type\":\"VString\",\"value\":\"s\"}"`,
		` { "type" : "VString" , "value" : "sp" } `, "{\"type\":\"VString\",\"value\":\"tab\tin\"}"}
	ints := []string{`{"type":"VInt","value":42}`, `{"type":"VInt","value":-7}`, `{"type":"VInt","value":-0}`, `{"type":"VInt","value":01}`,
		`{"type":"VInt","value":1.0}`, `{"type":"VInt","value":1e2}`, `{"type":"VInt","value":99999999999999999999}`,
		`{"type":"VString","value":"1"}`, `{"value":5,"type":"VInt"}`}
	lists := []string{`{"type":"VList","value":[]}`, `{"type":"VList","value":[{"type":"VInt","value":1},{"type":"VInt","value":2}]}`,
		`{"type":"VOption","value":{"type":"VList","value":[{"type":"VInt","value":3}]}}`, `{"type":"VOption","value":null}`,
		`{"type":"VList","value":[{"type":"VString","value":"a"}]}`, `{"type":"VTuple","value":[]}`, `{"type":"VList","value":[ ]}`}
	r := rand.New(rand.NewSource(6))
	pick := func(xs []string) string { return xs[r.Intn(len(xs))] }
	var payloads []string
	for i := 0; i < 20000; i++ {
		var items []string
		n := r.Intn(4)
		for j := 0; j < n; j++ {
			switch r.Intn(4) {
			case 0:
				items = append(items, pick(node))
			case 1:
				items = append(items, pick(keys))
			case 2:
				items = append(items, pick(ints))
			default:
				items = append(items, pick(lists))
			}
		}
		sep := []string{",", " , ", ",\n"}[r.Intn(3)]
		p := "[" + strings.Join(items, sep) + "]"
		switch r.Intn(20) {
		case 0:
			p = ""
		case 1:
			p = p + "x"
		case 2:
			p = p[:len(p)-1]
		case 3:
			p = "null"
		case 4:
			p = " " + p + " "
		}
		payloads = append(payloads, p)
	}
	payloads = append(payloads,
		`[{"type":"VNode","value":{"index":0,"role":9}},{"type":"VString","value":"key1"},{"type":"VInt","value":1}]`,
		`[{"type":"VList","value":[{"type":"VInt","value":1}]}]`, `[]`, `[ ]`, `[1,+2]`, `[1.5e3,-0.0]`, `[.5]`, `[true,false,null]`, `[nul]`)
	accepted := 0
	for _, p := range payloads {
		for _, rk := range []RowKind{Invocation, Response} {
			for _, k := range []Kind{Write, Read, RMW} {
				fast := Row{Kind: rk}
				if !scanPayload(&fast, k, p) {
					continue
				}
				accepted++
				slow := decodePayload(Row{Kind: rk}, k, p)
				if slow.Malformed != "" || fast.Key != slow.Key || fast.HasKey != slow.HasKey || fast.UID != slow.UID ||
					fast.HasUID != slow.HasUID || fast.HasValue != slow.HasValue || !slices.Equal(fast.Value, slow.Value) {
					t.Fatalf("payload %q kind %v row %v: scanner %+v, decoder %+v", p, k, rk, fast, slow)
				}
			}
		}
	}
	if accepted < 1000 {
		t.Fatalf("the scanner accepted only %d cases", accepted)
	}
}
