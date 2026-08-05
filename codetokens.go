package main

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

// Path / API-shaped snippets that tokenization would otherwise destroy.
// Examples: /notifications/{id}, /api/v1/topics/, foo/{bar}/baz
var pathLikeRe = regexp.MustCompile(`(?i)(?:/[A-Za-z0-9_{}.\-]+)+|[A-Za-z0-9_.\-]*\{[A-Za-z0-9_.\-]+\}[A-Za-z0-9_/{}.\-]*`)

const (
	maxCodeTokens     = 400
	maxCodeTokenRunes = 180
)

func extractCodeTokens(parts ...string) []string {
	seen := make(map[string]struct{}, 64)
	out := make([]string, 0, 64)
	add := func(raw string) {
		t := strings.ToLower(strings.TrimSpace(raw))
		if t == "" {
			return
		}
		if utf8.RuneCountInString(t) > maxCodeTokenRunes {
			t = string([]rune(t)[:maxCodeTokenRunes])
		}
		if _, ok := seen[t]; ok {
			return
		}
		seen[t] = struct{}{}
		out = append(out, t)
		// Also store without leading slash so "notifications/{id}" matches.
		if strings.HasPrefix(t, "/") && len(t) > 1 {
			alt := t[1:]
			if _, ok := seen[alt]; !ok {
				seen[alt] = struct{}{}
				out = append(out, alt)
			}
		}
	}

	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		for _, m := range pathLikeRe.FindAllString(p, -1) {
			add(m)
			if len(out) >= maxCodeTokens {
				return out
			}
		}
	}
	return out
}

func withCodeTokens(doc *SolrDoc) {
	if doc == nil {
		return
	}
	doc.CodeTokens = extractCodeTokens(doc.Title, doc.Content, doc.Path)
}
