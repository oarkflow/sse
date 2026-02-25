package sse

import "unicode"

func validateIdentifier(value string, maxLen int) bool {
	if value == "" {
		return false
	}
	if maxLen > 0 && len(value) > maxLen {
		return false
	}
	for _, r := range value {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			continue
		}
		switch r {
		case '-', '_', '.', ':', '/':
			continue
		default:
			return false
		}
	}
	return true
}
