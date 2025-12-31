package checker

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/anishathalye/porcupine"
)

// ActionType represents the type of action performed on the data structure
type ActionType string

const (
	Read    ActionType = "read"
	Write   ActionType = "write"
	Delete  ActionType = "delete"
	Crash   ActionType = "crash"
	Recover ActionType = "recover"
	Timeout ActionType = "timeout"
)

func (e *ActionType) UnmarshalCSV(value string) error {
	switch {
	case strings.HasSuffix(value, "ClientInterface.Read"):
		*e = Read
	case strings.HasSuffix(value, "ClientInterface.Write"):
		*e = Write
	case strings.HasSuffix(value, "ClientInterface.Delete"):
		*e = Delete
	case strings.HasSuffix(value, "System.Crash"):
		*e = Crash
	case strings.HasSuffix(value, "System.Recover"):
		*e = Recover
	case strings.HasSuffix(value, "ClientInterface.SimulateTimeout"):
		*e = Timeout
	default:
		*e = "Unknown operation."
	}
	return nil
}

// EventRow represents a single row in the history CSV file
type EventRow struct {
	UniqueID string     `csv:"UniqueID"`
	ClientID string     `csv:"ClientID"`
	Kind     string     `csv:"Kind"`
	Action   ActionType `csv:"Action"`
	Payload  string     `csv:"Payload"`
}

type pendingInvocation struct {
	invRow   *EventRow
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

// BuildOperations converts a slice of EventRows into porcupine Operations.
// Deprecated: Use BuildOperationsWithAnnotations for visualization with system events.
func BuildOperations(eventRows []*EventRow) []porcupine.Operation {
	ops, _ := BuildOperationsWithAnnotations(eventRows)
	return ops
}

// BuildOperationsWithAnnotations converts a slice of EventRows into porcupine Operations
// and also returns annotations for system events (Crash, Recover, Timeout) to overlay
// on the visualization.
func BuildOperationsWithAnnotations(eventRows []*EventRow) ([]porcupine.Operation, []porcupine.Annotation) {
	var ops []porcupine.Operation
	var annotations []porcupine.Annotation
	pendingInvocations := make(map[string]pendingInvocation)

	for i, row := range eventRows {
		syntheticTime := int64(i + 1)

		switch row.Action {
		case Crash:
			nodeID := extractNodeID(row.Payload)
			annotations = append(annotations, porcupine.Annotation{
				Tag:             fmt.Sprintf("Node %d", nodeID),
				Start:           syntheticTime,
				Description:     "💥 Crash",
				Details:         fmt.Sprintf("Node %d crashed", nodeID),
				BackgroundColor: "#ff6b6b",
				TextColor:       "#ffffff",
			})
			continue
		case Recover:
			nodeID := extractNodeID(row.Payload)
			annotations = append(annotations, porcupine.Annotation{
				Tag:             fmt.Sprintf("Node %d", nodeID),
				Start:           syntheticTime,
				Description:     "🔄 Recover",
				Details:         fmt.Sprintf("Node %d recovered", nodeID),
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
				callTime: syntheticTime,
				clientID: clientID,
			}
			// Handle system events as annotations
			switch row.Action {
			case Timeout:
				nodeID := extractNodeID(row.Payload)
				annotations = append(annotations, porcupine.Annotation{
					Tag:             fmt.Sprintf("Node %d", nodeID),
					Start:           syntheticTime,
					Description:     "⏱️ Timeout",
					Details:         fmt.Sprintf("Node %d simulated timeout", nodeID),
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
			if invRow.Action != Read && invRow.Action != Write && invRow.Action != Delete {
				continue
			}

			// Parse payload arrays from both invocation and response
			invPayloads := parsePayloadArray(invRow.Payload)
			respPayloads := parsePayloadArray(respRow.Payload)

			var opInput interface{}
			var opOutput interface{}

			switch invRow.Action {
			case Write:
				// Write: Payload[0]=node, Payload[1]=key, Payload[2]=value
				if len(invPayloads) < 3 {
					log.Printf("Warning: Write invocation for UniqueID %s has insufficient payloads. Skipping.", row.UniqueID)
					continue
				}
				// Parse the key from JSON structure to get the actual string value
				keyVal := ParseValue(invPayloads[1])
				opInput = KVInput{
					Op:  "PUT",
					Key: keyVal.String(),
					Val: ParseValue(invPayloads[2]),
				}
				if len(respPayloads) > 0 {
					opOutput = respPayloads[0]
				}
			case Read:
				// Read: Payload[0]=node, Payload[1]=key
				if len(invPayloads) < 2 {
					log.Printf("Warning: Read invocation for UniqueID %s has insufficient payloads. Skipping.", row.UniqueID)
					continue
				}
				// Parse the key from JSON structure to get the actual string value
				keyVal := ParseValue(invPayloads[1])
				opInput = KVInput{
					Op:  "GET",
					Key: keyVal.String(),
					Val: Value{}, // Zero value for read input
				}
				if len(respPayloads) > 0 {
					opOutput = respPayloads[0]
				}
			case Delete:
				// Delete: Payload[0]=node, Payload[1]=key
				if len(invPayloads) < 2 {
					log.Printf("Warning: Delete invocation for UniqueID %s has insufficient payloads. Skipping.", row.UniqueID)
					continue
				}
				// Parse the key from JSON structure to get the actual string value
				keyVal := ParseValue(invPayloads[1])
				opInput = KVInput{
					Op:  "DELETE",
					Key: keyVal.String(),
					Val: Value{},
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

	return ops, annotations
}

// extractNodeID parses the node ID from the payload JSON array.
// System events have Payload[0] = node ID.
func extractNodeID(payloadStr string) int {
	payloads := parsePayloadArray(payloadStr)
	if len(payloads) == 0 {
		return -1
	}
	// The payload is typically {"type":"VNode","value":N} or just a number
	v := ParseValue(payloads[0])
	if v.Type == "VNode" || v.Type == "VInt" {
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
