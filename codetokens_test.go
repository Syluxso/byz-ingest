package main

import "testing"

func TestExtractCodeTokens(t *testing.T) {
	toks := extractCodeTokens("See GET /notifications/{id} and /api/v1/topics/list")
	joined := ""
	for _, t := range toks {
		joined += t + " "
	}
	if !contains(toks, "/notifications/{id}") && !contains(toks, "notifications/{id}") {
		t.Fatalf("missing notifications path token: %v", toks)
	}
	if !contains(toks, "/api/v1/topics/list") && !contains(toks, "api/v1/topics/list") {
		t.Fatalf("missing topics path token: %v", toks)
	}
}

func contains(ss []string, want string) bool {
	for _, s := range ss {
		if s == want {
			return true
		}
	}
	return false
}
