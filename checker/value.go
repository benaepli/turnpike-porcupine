package checker

import (
	"encoding/json"
	"fmt"
)

// Value is one recorded Spur value: its type tag and its JSON encoding.
type Value struct {
	Type string          `json:"type"`
	Raw  json.RawMessage `json:"value"`
}

// ParseValue converts a JSON string into a Value struct.
func ParseValue(s string) Value {
	var v Value
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		return Value{Type: "Error", Raw: []byte(fmt.Sprintf("%q", err.Error()))}
	}
	return v
}
