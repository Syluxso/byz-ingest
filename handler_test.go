package main

import "testing"

func TestBuildFileContent(t *testing.T) {
	ev := FileLifecycleEvent{
		Name:        "invoice.pdf",
		ContentType: "application/pdf",
		StorageKey:  "org/2026/invoice.pdf",
	}
	got := buildFileContent(ev)
	want := "invoice.pdf application/pdf org/2026/invoice.pdf"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestFirstNonEmpty(t *testing.T) {
	if firstNonEmpty("", "b") != "b" {
		t.Fatal("expected b")
	}
}
