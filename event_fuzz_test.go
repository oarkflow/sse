package sse

import (
	"strings"
	"testing"
)

func FuzzEventEncode(f *testing.F) {
	f.Add("id-1", "message", "hello")
	f.Add("", "", "line1\nline2")
	f.Add("abc", "custom", "")

	f.Fuzz(func(t *testing.T, id, eventType, data string) {
		e := &Event{
			ID:   id,
			Type: eventType,
			Data: data,
		}
		encoded := string(e.Encode())
		if !strings.HasSuffix(encoded, "\n\n") {
			t.Fatalf("encoded event must terminate with blank line: %q", encoded)
		}
		if e.ID != "" && !strings.Contains(encoded, "id: "+e.ID+"\n") {
			t.Fatalf("encoded event missing id field")
		}
		et := e.Type
		if et == "" {
			et = "message"
		}
		if !strings.Contains(encoded, "event: "+et+"\n") {
			t.Fatalf("encoded event missing event type")
		}
		for _, line := range strings.Split(e.Data, "\n") {
			want := "data: " + line + "\n"
			if !strings.Contains(encoded, want) {
				t.Fatalf("encoded event missing data line %q", line)
			}
		}
	})
}
