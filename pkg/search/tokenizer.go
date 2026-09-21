package search

import (
	"strings"
	"unicode"
)

var defaultStopWords = map[string]struct{}{
	"a": {}, "an": {}, "and": {}, "are": {}, "as": {}, "at": {},
	"be": {}, "by": {}, "for": {}, "from": {}, "has": {}, "he": {},
	"in": {}, "is": {}, "it": {}, "its": {}, "of": {}, "on": {},
	"or": {}, "that": {}, "the": {}, "to": {}, "was": {}, "were": {},
	"will": {}, "with": {},
}

// Tokenize processes raw text into a list of normalized, unique terms.
func Tokenize(text string) []string {
	if text == "" {
		return nil
	}

	seen := make(map[string]struct{})
	var tokens []string

	// Split on non-alphanumeric runes
	words := strings.FieldsFunc(text, func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '-'
	})

	for _, w := range words {
		cleaned := strings.ToLower(strings.Trim(w, "_-"))
		if len(cleaned) < 2 || len(cleaned) > 64 {
			continue
		}
		if _, stop := defaultStopWords[cleaned]; stop {
			continue
		}
		if _, exists := seen[cleaned]; !exists {
			seen[cleaned] = struct{}{}
			tokens = append(tokens, cleaned)
		}
	}

	return tokens
}
