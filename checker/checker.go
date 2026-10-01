package checker

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

// EventRow is one history row. Action is the action string as recorded,
// prefix included. Step and GlobalTime are zero where the source does not
// record them.
type EventRow struct {
	UniqueID   string `csv:"UniqueID"`
	ClientID   string `csv:"ClientID"`
	Kind       string `csv:"Kind"`
	Action     string `csv:"Action"`
	Payload    string `csv:"Payload"`
	Step       int64  `csv:"Step"`
	GlobalTime int64  `csv:"GlobalTime"`
}

// parsePayloadArray parses the JSON array string and returns a slice of string payloads
// Each payload is unquoted (if it was a JSON string) to get the raw content
func parsePayloadArray(payloadStr string) []string {
	if strings.TrimSpace(payloadStr) == "" {
		return []string{}
	}

	var rawPayloads []json.RawMessage
	if err := json.Unmarshal([]byte(payloadStr), &rawPayloads); err != nil {
		log.Fatalf("failed to parse payload array %q: %v", payloadStr, err)
	}

	payloads := make([]string, len(rawPayloads))
	for i, raw := range rawPayloads {
		// Try to unmarshal as a string first (to remove JSON string quotes)
		var str string
		if err := json.Unmarshal(raw, &str); err == nil {
			payloads[i] = str
		} else {
			// If it's not a string, keep it as-is
			payloads[i] = string(raw)
		}
	}
	return payloads
}

// NodeNames looks up the deployment's description of the node at a global
// index; the second result is false for a node the deployment does not
// describe, such as a client node.
type NodeNames func(index int) (NodeLabel, bool)

// nodeText returns the annotation tag for a node and the longer name used in
// annotation details. The global index stays in both so a label can be
// matched against logs and traces, which carry only the index.
func nodeText(names NodeNames, index int) (tag, who string) {
	if names != nil {
		if l, ok := names(index); ok {
			tag = fmt.Sprintf("%s[%d] (node %d)", l.Role, l.Ordinal, index)
			if l.Path != "" {
				return tag, fmt.Sprintf("%s at %s", tag, l.Path)
			}
			return tag, tag
		}
	}
	tag = fmt.Sprintf("Node %d", index)
	return tag, tag
}

// extractNodeID parses the node ID from the payload JSON array.
// System events have Payload[0] = node ID.
func extractNodeID(payloadStr string) int {
	payloads := parsePayloadArray(payloadStr)
	if len(payloads) == 0 {
		return -1
	}
	// The payload is typically {"type":"VNode","value":{"role":N, "index":M}} or just a number
	v := ParseValue(payloads[0])
	if v.Type == "VNode" {
		var nObj struct {
			Role  int `json:"role"`
			Index int `json:"index"`
		}
		if err := json.Unmarshal(v.Raw, &nObj); err == nil {
			return nObj.Index
		}
	} else if v.Type == "VInt" {
		var n int
		if err := json.Unmarshal(v.Raw, &n); err == nil {
			return n
		}
	}
	// Fallback: try to parse as plain integer
	var n int
	if err := json.Unmarshal([]byte(payloads[0]), &n); err == nil {
		return n
	}
	return -1
}
