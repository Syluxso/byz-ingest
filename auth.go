package main

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/MicahParks/keyfunc/v3"
	"github.com/golang-jwt/jwt/v5"
)

func withCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, X-Request-Id")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func withJWT(k keyfunc.Keyfunc, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		if !strings.HasPrefix(h, "Bearer ") {
			writeProblem(w, http.StatusUnauthorized, "Unauthorized", "missing bearer token")
			return
		}
		tokenStr := strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
		tok, err := jwt.Parse(tokenStr, k.Keyfunc,
			jwt.WithValidMethods([]string{"RS256"}),
			jwt.WithExpirationRequired(),
		)
		if err != nil || !tok.Valid {
			writeProblem(w, http.StatusUnauthorized, "Unauthorized", "invalid token")
			return
		}
		next(w, r)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}

func writeProblem(w http.ResponseWriter, status int, title, detail string) {
	writeJSON(w, status, map[string]any{
		"title":  title,
		"detail": detail,
		"status": status,
	})
}
