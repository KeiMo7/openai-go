package openai_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	openai "github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/internal/streamconfig"
	"github.com/openai/openai-go/v3/internal/streamrecovery"
	"github.com/openai/openai-go/v3/option"
)

// recoveryScript is one handler response in a scripted SSE failure scenario.
type recoveryScript struct {
	status       int
	transportErr bool
	sse          string
	wantResume   string
	prelude      string
}

// scriptedRecoveryServer serves one logical stream across several physical
// connections. Each logical stream carries a unique X-Test-Stream header on
// every attempt, so the server tracks per-stream attempt indices regardless of
// TCP connection reuse; concurrent streams never share state.
func scriptedRecoveryServer(t *testing.T, scripts []recoveryScript) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var total atomic.Int32
	var mu sync.Mutex
	perStream := map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		streamID := r.Header.Get("X-Test-Stream")
		if streamID == "" {
			t.Error("request missing X-Test-Stream")
			http.Error(w, "no stream id", http.StatusInternalServerError)
			return
		}
		mu.Lock()
		n := perStream[streamID]
		perStream[streamID] = n + 1
		mu.Unlock()
		total.Add(1)

		resume := r.Header.Get("Last-Event-ID")
		if got := r.Header.Get("Authorization"); got != "Bearer synthetic-key" {
			t.Errorf("attempt %d Authorization=%q", n, got)
		}
		if n >= len(scripts) {
			t.Errorf("unexpected attempt %d", n)
			http.Error(w, "no script", http.StatusInternalServerError)
			return
		}
		s := scripts[n]
		if s.wantResume != "" && resume != s.wantResume {
			t.Errorf("attempt %d Last-Event-ID=%q, want %q", n, resume, s.wantResume)
		}
		if s.status >= 400 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(s.status)
			_, _ = io.WriteString(w, `{"error":{"message":"scripted","type":"server_error"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		if s.transportErr {
			if s.prelude != "" {
				_, _ = io.WriteString(w, s.prelude)
				flusher.Flush()
			}
			hj, ok := w.(http.Hijacker)
			if !ok {
				t.Error("test server cannot hijack connection")
				return
			}
			conn, _, _ := hj.Hijack()
			_ = conn.Close()
			return
		}
		_, _ = io.WriteString(w, s.sse)
		flusher.Flush()
	}))
	t.Cleanup(server.Close)
	return server, &total
}

func sseChunk(id, content string, finish bool, usage bool) string {
	finishField := ""
	if finish {
		finishField = `,"finish_reason":"stop"`
	}
	usageField := ""
	if usage {
		usageField = `,"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}`
	}
	payload := `{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-test","choices":[{"index":0,"delta":{"content":"` + content + `"}` + finishField + `}]` + usageField + `}`
	return "id: " + id + "\ndata: " + payload + "\n\n"
}

func sseChunkNoID(content string) string {
	payload := `{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"gpt-test","choices":[{"index":0,"delta":{"content":"` + content + `"},"finish_reason":null}]}`
	return "data: " + payload + "\n\n"
}

func sseDone() string { return "data: [DONE]\n\n" }

var testStreamSeq atomic.Int64

// nextTestStreamID returns a process-unique identifier for one logical stream.
func nextTestStreamID() string {
	return strconv.FormatInt(testStreamSeq.Add(1), 10)
}

func recoveryClient(t *testing.T, url string, onEvent func(streamconfig.Event), attempts int) openai.Client {
	t.Helper()
	return openai.NewClient(
		option.WithBaseURL(url),
		option.WithAPIKey("synthetic-key"),
		option.WithMaxRetries(0),
		option.WithMaxRetryDelay(200*time.Millisecond),
		option.WithStreamRecovery(option.StreamRecoveryConfig{
			Enabled:     true,
			MaxAttempts: attempts,
			MaxDelay:    200 * time.Millisecond,
			OnEvent:     onEvent,
		}),
	)
}

func chatStreamRequest() openai.ChatCompletionNewParams {
	return openai.ChatCompletionNewParams{
		Messages: []openai.ChatCompletionMessageParamUnion{{
			OfUser: &openai.ChatCompletionUserMessageParam{
				Content: openai.ChatCompletionUserMessageParamContentUnion{OfString: openai.String("hello")},
			},
		}},
		Model: "gpt-test",
	}
}

func TestStreamRecoveryDisabledPreservesBehavior(t *testing.T) {
	server, count := scriptedRecoveryServer(t, []recoveryScript{
		{transportErr: true, prelude: sseChunk("1", "a", false, false)},
	})
	var observed atomic.Int32
	client := openai.NewClient(
		option.WithBaseURL(server.URL),
		option.WithAPIKey("synthetic-key"),
		option.WithMaxRetries(0),
		option.WithHeader("X-Test-Stream", "disabled"),
		option.WithStreamRecovery(option.StreamRecoveryConfig{Enabled: false,
			OnEvent: func(streamconfig.Event) { observed.Add(1) }}),
	)
	stream := client.Chat.Completions.NewStreaming(t.Context(), chatStreamRequest(), option.WithHeader("X-Test-Stream", nextTestStreamID()))
	var pieces int
	for stream.Next() {
		pieces++
	}
	if stream.Err() == nil {
		t.Fatal("disabled recovery must still surface the connection drop")
	}
	if count.Load() != 1 {
		t.Fatalf("attempts=%d, want exactly the initial connection", count.Load())
	}
	if observed.Load() != 0 {
		t.Fatalf("observed=%d, want zero events when disabled", observed.Load())
	}
}

func TestStreamRecoveryResendBeforeFirstFragment(t *testing.T) {
	server, count := scriptedRecoveryServer(t, []recoveryScript{
		{transportErr: true},
		{sse: sseChunk("1", "a", false, false) + sseChunk("2", "b", true, true) + sseDone()},
	})
	var events []streamconfig.Event
	client := recoveryClient(t, server.URL, func(e streamconfig.Event) { events = append(events, e) }, 2)
	stream := client.Chat.Completions.NewStreaming(t.Context(), chatStreamRequest(), option.WithHeader("X-Test-Stream", nextTestStreamID()))
	var acc openai.ChatCompletionAccumulator
	for stream.Next() {
		if !acc.AddChunk(stream.Current()) {
			t.Fatal("accumulator rejected chunk")
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 2 {
		t.Fatalf("connections=%d, want 2", count.Load())
	}
	if acc.Choices[0].Message.Content != "ab" {
		t.Fatalf("content=%q, want ab", acc.Choices[0].Message.Content)
	}
	if acc.Usage.TotalTokens != 7 {
		t.Fatalf("usage=%#v", acc.Usage)
	}
	if len(events) != 1 || events[0].Attempt != 1 || events[0].Resume || events[0].Reason != string(streamrecovery.ReasonConnectionEnd) {
		t.Fatalf("events=%#v", events)
	}
}

func TestStreamRecoveryRenewalDeduplicatesOverlap(t *testing.T) {
	server, count := scriptedRecoveryServer(t, []recoveryScript{
		{transportErr: true, prelude: sseChunk("1", "a", false, false) + sseChunk("2", "b", false, false)},
		{wantResume: "2", sse: sseChunk("2", "b", false, false) + sseChunk("3", "c", true, true) + sseDone()},
	})
	var events []streamconfig.Event
	client := recoveryClient(t, server.URL, func(e streamconfig.Event) { events = append(events, e) }, 2)
	stream := client.Chat.Completions.NewStreaming(t.Context(), chatStreamRequest(), option.WithHeader("X-Test-Stream", nextTestStreamID()))
	var acc openai.ChatCompletionAccumulator
	for stream.Next() {
		if !acc.AddChunk(stream.Current()) {
			t.Fatal("accumulator rejected chunk")
		}
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 2 {
		t.Fatalf("connections=%d, want 2", count.Load())
	}
	if acc.Choices[0].Message.Content != "abc" {
		t.Fatalf("content=%q, want abc (overlap deduped)", acc.Choices[0].Message.Content)
	}
	if acc.Usage.TotalTokens != 7 {
		t.Fatalf("usage=%#v, counted once", acc.Usage)
	}
	if len(events) != 1 || !events[0].Resume || events[0].Position != "2" {
		t.Fatalf("events=%#v", events)
	}
}

func TestStreamRecoveryByteEquivalenceWithCleanPath(t *testing.T) {
	clean := sseChunk("1", "a", false, false) + sseChunk("2", "b", false, false) +
		sseChunk("3", "c", true, true) + sseDone()

	server, _ := scriptedRecoveryServer(t, []recoveryScript{
		{transportErr: true, prelude: sseChunk("1", "a", false, false) + sseChunk("2", "b", false, false)},
		{wantResume: "2", sse: sseChunk("2", "b", false, false) + sseChunk("3", "c", true, true) + sseDone()},
	})
	client := recoveryClient(t, server.URL, nil, 2)

	recAcc := openai.ChatCompletionAccumulator{}
	recStream := client.Chat.Completions.NewStreaming(t.Context(), chatStreamRequest(), option.WithHeader("X-Test-Stream", nextTestStreamID()))
	for recStream.Next() {
		if !recAcc.AddChunk(recStream.Current()) {
			t.Fatal("recovered accumulator rejected chunk")
		}
	}
	if err := recStream.Err(); err != nil {
		t.Fatal(err)
	}

	cleanServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, clean)
	}))
	t.Cleanup(cleanServer.Close)
	cleanClient := openai.NewClient(option.WithBaseURL(cleanServer.URL), option.WithAPIKey("synthetic-key"), option.WithMaxRetries(0))
	cleanAcc := openai.ChatCompletionAccumulator{}
	cleanStream := cleanClient.Chat.Completions.NewStreaming(t.Context(), chatStreamRequest())
	for cleanStream.Next() {
		if !cleanAcc.AddChunk(cleanStream.Current()) {
			t.Fatal("clean accumulator rejected chunk")
		}
	}
	if err := cleanStream.Err(); err != nil {
		t.Fatal(err)
	}

	if recAcc.Choices[0].Message.Content != cleanAcc.Choices[0].Message.Content {
		t.Fatalf("content mismatch recovered=%q clean=%q", recAcc.Choices[0].Message.Content, cleanAcc.Choices[0].Message.Content)
	}
	if !reflect.DeepEqual(recAcc.Usage, cleanAcc.Usage) {
		t.Fatalf("usage mismatch recovered=%#v clean=%#v", recAcc.Usage, cleanAcc.Usage)
	}
	if recAcc.Choices[0].FinishReason != cleanAcc.Choices[0].FinishReason {
		t.Fatalf("finish mismatch %q vs %q", recAcc.Choices[0].FinishReason, cleanAcc.Choices[0].FinishReason)
	}
}

func TestStreamRecoveryNoPositionReturnsPartialNonResumable(t *testing.T) {
	server, count := scriptedRecoveryServer(t, []recoveryScript{
		{transportErr: true, prelude: sseChunkNoID("a") + sseChunkNoID("b")},
	})
	client := recoveryClient(t, server.URL, nil, 2)
	stream := client.Chat.Completions.NewStreaming(t.Context(), chatStreamRequest(), option.WithHeader("X-Test-Stream", nextTestStreamID()))
	var pieces int
	for stream.Next() {
		pieces++
	}
	var term *streamrecovery.TerminalError
	if !errors.As(stream.Err(), &term) || term.Code != streamrecovery.ErrCodeNoResume {
		t.Fatalf("err=%v, want non-resumable", stream.Err())
	}
	if pieces != 2 {
		t.Fatalf("partial pieces=%d, want 2 retained", pieces)
	}
	if count.Load() != 1 {
		t.Fatalf("connections=%d, must not resend without a position", count.Load())
	}
}

func TestStreamRecoveryGapIsReported(t *testing.T) {
	server, _ := scriptedRecoveryServer(t, []recoveryScript{
		{transportErr: true, prelude: sseChunk("1", "a", false, false) + sseChunk("2", "b", false, false)},
		{wantResume: "2", sse: sseChunk("5", "e", true, true) + sseDone()},
	})
	client := recoveryClient(t, server.URL, nil, 2)
	stream := client.Chat.Completions.NewStreaming(t.Context(), chatStreamRequest(), option.WithHeader("X-Test-Stream", nextTestStreamID()))
	for stream.Next() {
	}
	var term *streamrecovery.TerminalError
	if !errors.As(stream.Err(), &term) || term.Code != streamrecovery.ErrCodeGap || term.GapSize != 2 {
		t.Fatalf("err=%v, want gap of 2", stream.Err())
	}
}

func TestStreamRecoveryBudgetExhaustion(t *testing.T) {
	server, count := scriptedRecoveryServer(t, []recoveryScript{
		{transportErr: true, prelude: sseChunk("1", "a", false, false)},
		{transportErr: true, prelude: sseChunk("2", "b", false, false)},
		{transportErr: true},
	})
	var events []streamconfig.Event
	client := recoveryClient(t, server.URL, func(e streamconfig.Event) { events = append(events, e) }, 2)
	stream := client.Chat.Completions.NewStreaming(t.Context(), chatStreamRequest(), option.WithHeader("X-Test-Stream", nextTestStreamID()))
	var pieces int
	for stream.Next() {
		pieces++
	}
	var term *streamrecovery.TerminalError
	if !errors.As(stream.Err(), &term) || term.Code != streamrecovery.ErrCodeBudgetExhausted {
		t.Fatalf("err=%v, want budget exhausted", stream.Err())
	}
	if pieces != 2 {
		t.Fatalf("partial pieces=%d, delivered fragments retained", pieces)
	}
	if count.Load() != 3 {
		t.Fatalf("connections=%d, want initial + 2 attempts", count.Load())
	}
	if len(events) != 2 || events[0].Attempt != 1 || events[1].Attempt != 2 || !events[1].BudgetExhausted {
		t.Fatalf("events=%#v, want two consecutive attempts ending exhausted", events)
	}
	for _, e := range events {
		if e.Waited > 200*time.Millisecond {
			t.Fatalf("wait %v exceeds configured max", e.Waited)
		}
	}
}

func TestStreamRecoveryNonRetryableStatusStops(t *testing.T) {
	server, count := scriptedRecoveryServer(t, []recoveryScript{
		{transportErr: true, prelude: sseChunk("1", "a", false, false)},
		{status: http.StatusUnauthorized},
		{sse: sseDone()},
	})
	client := recoveryClient(t, server.URL, nil, 3)
	stream := client.Chat.Completions.NewStreaming(t.Context(), chatStreamRequest(), option.WithHeader("X-Test-Stream", nextTestStreamID()))
	var pieces int
	for stream.Next() {
		pieces++
	}
	var term *streamrecovery.TerminalError
	if !errors.As(stream.Err(), &term) || term.Code != streamrecovery.ErrCodeNonRetryableStatus {
		t.Fatalf("err=%v, want non-retryable status", stream.Err())
	}
	if pieces != 1 {
		t.Fatalf("pieces=%d, partial result preserved", pieces)
	}
	if count.Load() != 2 {
		t.Fatalf("connections=%d, must stop without further backoff", count.Load())
	}
}

func TestStreamRecoveryCancellationEndsWait(t *testing.T) {
	server, _ := scriptedRecoveryServer(t, []recoveryScript{
		{transportErr: true, prelude: sseChunk("1", "a", false, false)},
		{sse: sseDone()},
	})
	client := openai.NewClient(
		option.WithBaseURL(server.URL),
		option.WithAPIKey("synthetic-key"),
		option.WithMaxRetries(0),
		option.WithMaxRetryDelay(30*time.Second),
		option.WithHeader("X-Test-Stream", "cancel"),
		option.WithStreamRecovery(option.StreamRecoveryConfig{Enabled: true, MaxAttempts: 2, MaxDelay: 30 * time.Second}),
	)
	ctx, cancel := context.WithCancel(context.Background())
	stream := client.Chat.Completions.NewStreaming(ctx, chatStreamRequest(), option.WithHeader("X-Test-Stream", nextTestStreamID()))
	if !stream.Next() {
		t.Fatalf("first fragment expected: %v", stream.Err())
	}
	cancel()
	start := time.Now()
	for stream.Next() {
	}
	elapsed := time.Since(start)
	if !errors.Is(stream.Err(), context.Canceled) {
		t.Fatalf("err=%v, want context.Canceled", stream.Err())
	}
	if elapsed > time.Second {
		t.Fatalf("cancellation took %v, wait was not interrupted", elapsed)
	}
}

func TestStreamRecoveryConcurrentStreamsDoNotInterfere(t *testing.T) {
	const streams = 8
	server, count := scriptedRecoveryServer(t, []recoveryScript{
		{transportErr: true, prelude: sseChunk("1", "a", false, false)},
		{sse: sseChunk("1", "a", false, false) + sseChunk("2", "b", true, true) + sseDone()},
	})
	client := recoveryClient(t, server.URL, nil, 2)
	var wg sync.WaitGroup
	errs := make([]error, streams)
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			stream := client.Chat.Completions.NewStreaming(t.Context(), chatStreamRequest(), option.WithHeader("X-Test-Stream", nextTestStreamID()))
			var acc openai.ChatCompletionAccumulator
			for stream.Next() {
				acc.AddChunk(stream.Current())
			}
			if stream.Err() != nil {
				errs[idx] = stream.Err()
				return
			}
			if acc.Choices[0].Message.Content != "ab" {
				errs[idx] = errors.New("wrong content: " + acc.Choices[0].Message.Content)
			}
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("stream %d: %v", i, err)
		}
	}
	if got := count.Load(); got != streams*2 {
		t.Fatalf("connections=%d, want %d (two per independent stream)", got, streams*2)
	}
}

func TestStreamRecoveryObservationLeaksNoPayloadOrCredentials(t *testing.T) {
	server, _ := scriptedRecoveryServer(t, []recoveryScript{
		{transportErr: true, prelude: sseChunk("1", "secret-fragment", false, false)},
		{sse: sseChunk("2", "b", true, true) + sseDone()},
	})
	var seen strings.Builder
	client := openai.NewClient(
		option.WithBaseURL(server.URL),
		option.WithAPIKey("synthetic-key"),
		option.WithMaxRetries(0),
		option.WithHeader("X-Test-Stream", "leak"),
		option.WithStreamRecovery(option.StreamRecoveryConfig{
			Enabled: true, MaxAttempts: 2, MaxDelay: 50 * time.Millisecond,
			OnEvent: func(e streamconfig.Event) {
				seen.WriteString(e.Position)
				seen.WriteString(e.Reason)
			},
		}),
	)
	stream := client.Chat.Completions.NewStreaming(t.Context(), chatStreamRequest(), option.WithHeader("X-Test-Stream", nextTestStreamID()))
	for stream.Next() {
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(seen.String(), "secret-fragment") {
		t.Fatal("observation exposed fragment payload")
	}
	if strings.Contains(seen.String(), "synthetic-key") || strings.Contains(seen.String(), "Bearer") {
		t.Fatal("observation exposed credentials")
	}
}
