package api

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

var parseBenchmarkEvent any
var benchmarkReceived = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

func BenchmarkParseEvent(b *testing.B) {
	for name, body := range map[string]string{
		"heartbeat":      `{"device_id":"dev_0001","room_id":"room_001","type":"heartbeat","ts":"2026-11-15T12:00:00.123Z","seq":12345}`,
		"motion":         `{"device_id":"dev_0001","room_id":"room_001","type":"motion","ts":"2026-11-15T12:00:00.123Z","seq":12345,"magnitude":0.42}`,
		"unknown_nested": `{"device_id":"dev_0001","room_id":"room_001","type":"motion","ts":"2026-11-15T12:00:00.123Z","seq":12345,"magnitude":0.42,"extra":{"array":[true,0.50,{"escaped":"<>&\\text"}],"zero":0e999999}}`,
	} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				event, err := parseEvent(strings.NewReader(body), benchmarkReceived)
				if err != nil {
					b.Fatal(err)
				}
				parseBenchmarkEvent = event
			}
		})
	}
}

type discardResponseWriter struct{ header http.Header }

func (w *discardResponseWriter) Header() http.Header       { return w.header }
func (*discardResponseWriter) WriteHeader(int)             {}
func (*discardResponseWriter) Write(p []byte) (int, error) { return len(p), nil }

func BenchmarkEventSuccessJSON(b *testing.B) {
	w := &discardResponseWriter{header: make(http.Header)}
	b.ReportAllocs()
	for b.Loop() {
		writeEventSuccess(w, false)
	}
}
