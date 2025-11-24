package checker

import (
	"encoding/json"
	"log"
	"strconv"
	"strings"

	"github.com/anishathalye/porcupine"
)

// ActionType represents the type of action performed on the data structure
type ActionType string

const (
	Read   ActionType = "read"
	Write  ActionType = "write"
	Delete ActionType = "delete"
)

func (e *ActionType) UnmarshalCSV(value string) error {
	switch {
	case strings.HasSuffix(value, "ClientInterface.Read"):
		*e = Read
	case strings.HasSuffix(value, "ClientInterface.Write"):
		*e = Write
	case strings.HasSuffix(value, "ClientInterface.Delete"):
		*e = Delete
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

// BuildOperations converts a slice of EventRows into porcupine Operations
func BuildOperations(eventRows []*EventRow) []porcupine.Operation {
	var ops []porcupine.Operation
	pendingInvocations := make(map[string]pendingInvocation)

	for i, row := range eventRows {
		syntheticTime := int64(i + 1)

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

	return ops
}
