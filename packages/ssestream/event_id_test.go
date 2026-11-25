package ssestream

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestDecoderParsesEventID(t *testing.T) {
	body := "id: 42\nevent: chunk\ndata: {\"v\":1}\n\n" +
		"event: chunk\ndata: {\"v\":2}\n\n"
	res := &http.Response{
		Header: http.Header{"Content-Type": {"text/event-stream"}},
		Body:   io.NopCloser(strings.NewReader(body)),
	}
	dec := NewDecoder(res)

	if !dec.Next() {
		t.Fatalf("first event missing: %v", dec.Err())
	}
	if got := dec.Event().ID; got != "42" {
		t.Fatalf("id=%q, want 42", got)
	}
	if got := dec.Event().Type; got != "chunk" {
		t.Fatalf("type=%q, want chunk", got)
	}

	if !dec.Next() {
		t.Fatal("second event missing")
	}
	if got := dec.Event().ID; got != "" {
		t.Fatalf("id without id: field must be empty, got %q", got)
	}
}

func TestDecoderResetsIDAcrossEmptyEvents(t *testing.T) {
	// An empty dispatch between events must not leak the previous event's id.
	body := "id: 7\ndata: {\"v\":1}\n\n: keep-alive\n\ndata: {\"v\":2}\n\n"
	res := &http.Response{
		Header: http.Header{"Content-Type": {"text/event-stream"}},
		Body:   io.NopCloser(strings.NewReader(body)),
	}
	dec := NewDecoder(res)

	if !dec.Next() || dec.Event().ID != "7" {
		t.Fatalf("first id=%q", dec.Event().ID)
	}
	if !dec.Next() {
		t.Fatal("second event missing")
	}
	if got := dec.Event().ID; got != "" {
		t.Fatalf("id leaked across empty event: %q", got)
	}
}
