package sse

import "testing"

func FuzzValidateIdentifier(f *testing.F) {
	f.Add("topic-1", 64)
	f.Add("a/b:c_d.e", 32)
	f.Add("bad space", 64)
	f.Add("", 64)

	f.Fuzz(func(t *testing.T, value string, maxLen int) {
		if maxLen < 0 {
			maxLen = 0
		}
		got := validateIdentifier(value, maxLen)
		if value == "" && got {
			t.Fatalf("empty identifiers must be invalid")
		}
		if maxLen > 0 && len(value) > maxLen && got {
			t.Fatalf("identifier exceeding maxLen must be invalid")
		}
	})
}
