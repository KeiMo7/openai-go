package streamrecovery

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/openai/openai-go/v3/packages/ssestream"
)

// scriptedOpener replays canned SSE responses per attempt and records how it
// was called.
type scriptedOpener struct {
	responses []scriptedResponse
	calls     []openCall
}

type scriptedResponse struct {
	status int
	body   string
	err    error
}

type openCall struct {
	position string
}

func (o *scriptedOpener) Open(_ context.Context, position string) (*http.Response, error) {
	o.calls = append(o.calls, openCall{position: position})
	idx := len(o.calls) - 1
	if idx >= len(o.responses) {
		return nil, errors.New("scriptedOpener: out of scripted responses")
	}
	r := o.responses[idx]
	if r.err != nil {
		return nil, r.err
	}
	status := r.status
	if status == 0 {
		status = http.StatusOK
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(r.body)),
	}, nil
}

func newPlainDecoder(resp *http.Response) Decoder {
	return ssestream.NewDecoder(resp)
}

func newTestController(t *testing.T, cfg Config, opener Opener, firstBody string) *Controller {
	t.Helper()
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(firstBody)),
	}
	dec := ssestream.NewDecoder(resp)
	c := NewController(context.Background(), cfg, opener, resp, dec, newPlainDecoder)
	// Deterministic, jitter-free, near-instant backoff for tests.
	c.sleep = func(ctx context.Context, d time.Duration) error {
		// Honor cancellation immediately but do not actually wait.
		return ctx.Err()
	}
	c.backoff = func(_ int, _ time.Duration) time.Duration { return time.Millisecond }
	return c
}

func sse(id, data string) string {
	if id != "" {
		return "id: " + id + "\ndata: " + data + "\n\n"
	}
	return "data: " + data + "\n\n"
}

func drain(t *testing.T, c *Controller) []ssestream.Event {
	t.Helper()
	var got []ssestream.Event
	for c.Next() {
		got = append(got, c.Event())
	}
	return got
}

func TestCleanTerminalMarkerEndsWithoutRecovery(t *testing.T) {
	opener := &scriptedOpener{}
	body := sse("1", `{"v":"a"}`) + sse("2", `{"v":"b"}`) + "data: [DONE]\n\n"
	c := newTestController(t, Config{MaxAttempts: 2}, opener, body)

	got := drain(t, c)
	if err := c.Err(); err != nil {
		t.Fatalf("clean marker produced error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("events=%d, want 2", len(got))
	}
	if len(opener.calls) != 0 {
		t.Fatalf("recovery attempts=%d, want 0", len(opener.calls))
	}
	if c.Stats().Attempts != 0 {
		t.Fatalf("stats attempts=%d, want 0", c.Stats().Attempts)
	}
}

func TestEarlyCloseBeforeFirstFragmentResendsWholeRequest(t *testing.T) {
	opener := &scriptedOpener{
		responses: []scriptedResponse{
			{body: sse("1", `{"v":"a"}`) + sse("2", `{"v":"b"}`) + "data: [DONE]\n\n"},
		},
	}
	c := newTestController(t, Config{MaxAttempts: 2}, opener, "id: 0\ndata: {\"v\":\"ignored\"}\n\n")
	// The first body deliberately ends without [DONE] and before Next is even
	// called; build a fresh controller whose body truncates immediately.
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader("")),
	}
	c = NewController(context.Background(), Config{MaxAttempts: 2}, opener, resp,
		ssestream.NewDecoder(resp), newPlainDecoder)
	c.sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	c.backoff = func(_ int, _ time.Duration) time.Duration { return time.Millisecond }

	got := drain(t, c)
	if err := c.Err(); err != nil {
		t.Fatalf("resend failed: %v", err)
	}
	if len(got) != 2 || strings.TrimSpace(string(got[0].Data)) != `{"v":"a"}` {
		t.Fatalf("events=%v", got)
	}
	if len(opener.calls) != 1 || opener.calls[0].position != "" {
		t.Fatalf("calls=%v, want one full resend with empty position", opener.calls)
	}
	if c.Stats().FullResends != 1 || c.Stats().Renewals != 0 {
		t.Fatalf("stats=%#v", c.Stats())
	}
}

func TestRenewalAfterDeliveredFragmentsUsesPosition(t *testing.T) {
	opener := &scriptedOpener{
		responses: []scriptedResponse{
			// Renewal resends positions 2 (overlap) then 3, done.
			{body: sse("2", `{"v":"b"}`) + sse("3", `{"v":"c"}`) + "data: [DONE]\n\n"},
		},
	}
	first := sse("1", `{"v":"a"}`) + sse("2", `{"v":"b"}`)
	c := newTestController(t, Config{MaxAttempts: 2}, opener, first)

	got := drain(t, c)
	if err := c.Err(); err != nil {
		t.Fatalf("renewal failed: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("events=%d, want 3 deduped events", len(got))
	}
	want := []string{"1", "2", "3"}
	for i, e := range got {
		if e.ID != want[i] {
			t.Fatalf("event %d id=%q, want %q", i, e.ID, want[i])
		}
	}
	if len(opener.calls) != 1 || opener.calls[0].position != "2" {
		t.Fatalf("calls=%v, want one renewal at position 2", opener.calls)
	}
	stats := c.Stats()
	if stats.Renewals != 1 || stats.FullResends != 0 || stats.DuplicateFragments != 1 {
		t.Fatalf("stats=%#v", stats)
	}
}

func TestNoPositionAfterDeliveryReturnsPartialNonResumable(t *testing.T) {
	opener := &scriptedOpener{}
	// Events carry no id field.
	first := "data: {\"v\":\"a\"}\n\ndata: {\"v\":\"b\"}\n\n"
	c := newTestController(t, Config{MaxAttempts: 2}, opener, first)

	got := drain(t, c)
	var term *TerminalError
	if !errors.As(c.Err(), &term) || term.Code != ErrCodeNoResume {
		t.Fatalf("err=%v, want ErrCodeNoResume", c.Err())
	}
	if len(got) != 2 {
		t.Fatalf("partial events=%d, want the 2 delivered fragments retained", len(got))
	}
	if len(opener.calls) != 0 {
		t.Fatalf("must not resend without a position, calls=%v", opener.calls)
	}
}

func TestGapIsDetectedAndReported(t *testing.T) {
	opener := &scriptedOpener{
		responses: []scriptedResponse{
			// Jumps from position 2 to 5: gap of two (3,4).
			{body: sse("5", `{"v":"e"}`) + "data: [DONE]\n\n"},
		},
	}
	first := sse("1", `{"v":"a"}`) + sse("2", `{"v":"b"}`)
	c := newTestController(t, Config{MaxAttempts: 2}, opener, first)

	_ = drain(t, c)
	var term *TerminalError
	if !errors.As(c.Err(), &term) || term.Code != ErrCodeGap || term.GapSize != 2 {
		t.Fatalf("err=%v, want gap of 2", c.Err())
	}
	if c.Stats().Gaps != 2 {
		t.Fatalf("gap stats=%d, want 2", c.Stats().Gaps)
	}
}

func TestBackwardPositionIsDiscardedAsDuplicate(t *testing.T) {
	opener := &scriptedOpener{
		responses: []scriptedResponse{
			// Replays 1,2 then continues with 3.
			{body: sse("1", `{"v":"a"}`) + sse("2", `{"v":"b"}`) + sse("3", `{"v":"c"}`) + "data: [DONE]\n\n"},
		},
	}
	first := sse("1", `{"v":"a"}`) + sse("2", `{"v":"b"}`)
	c := newTestController(t, Config{MaxAttempts: 2}, opener, first)

	got := drain(t, c)
	if err := c.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("events=%d, want 3 after discarding backward overlap", len(got))
	}
	if c.Stats().DuplicateFragments != 2 {
		t.Fatalf("duplicates=%d, want 2", c.Stats().DuplicateFragments)
	}
}

func TestBudgetExhaustionReturnsPartialAndReason(t *testing.T) {
	opener := &scriptedOpener{
		responses: []scriptedResponse{
			{err: errors.New("reset")},
			{err: errors.New("reset again")},
		},
	}
	first := sse("1", `{"v":"a"}`) + sse("2", `{"v":"b"}`)
	c := newTestController(t, Config{MaxAttempts: 2}, opener, first)

	got := drain(t, c)
	var term *TerminalError
	if !errors.As(c.Err(), &term) || term.Code != ErrCodeBudgetExhausted {
		t.Fatalf("err=%v, want budget exhausted", c.Err())
	}
	if len(got) != 2 {
		t.Fatalf("partial fragments=%d, want 2 retained", len(got))
	}
	if c.Stats().Renewals != 2 {
		t.Fatalf("renewals=%d, want 2", c.Stats().Renewals)
	}
}

func TestNonRetryableStatusStopsImmediately(t *testing.T) {
	opener := &scriptedOpener{
		responses: []scriptedResponse{
			{status: http.StatusUnauthorized, body: "{}"},
			// A second scripted response exists to prove it is never reached.
			{body: "data: [DONE]\n\n"},
		},
	}
	first := sse("1", `{"v":"a"}`)
	c := newTestController(t, Config{MaxAttempts: 3}, opener, first)

	_ = drain(t, c)
	var term *TerminalError
	if !errors.As(c.Err(), &term) || term.Code != ErrCodeNonRetryableStatus {
		t.Fatalf("err=%v, want non-retryable status", c.Err())
	}
	if len(opener.calls) != 1 {
		t.Fatalf("calls=%d, must stop after one non-retryable status", len(opener.calls))
	}
}

func TestRetryableStatusConsumesBudgetAndRecovers(t *testing.T) {
	opener := &scriptedOpener{
		responses: []scriptedResponse{
			{status: http.StatusServiceUnavailable, body: "{}"},
			{body: sse("2", `{"v":"b"}`) + "data: [DONE]\n\n"},
		},
	}
	first := sse("1", `{"v":"a"}`)
	c := newTestController(t, Config{MaxAttempts: 3}, opener, first)

	got := drain(t, c)
	if err := c.Err(); err != nil {
		t.Fatalf("recovery should succeed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("events=%d, want 2", len(got))
	}
	if len(opener.calls) != 2 {
		t.Fatalf("calls=%d, want 2 (status then renewal)", len(opener.calls))
	}
}

func TestCancellationEndsWaitImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	opener := &scriptedOpener{
		responses: []scriptedResponse{{body: "data: [DONE]\n\n"}},
	}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader("")),
	}
	c := NewController(ctx, Config{MaxAttempts: 2}, opener, resp,
		ssestream.NewDecoder(resp), newPlainDecoder)
	started := make(chan struct{})
	c.sleep = func(ctx context.Context, _ time.Duration) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}

	cancel()
	if c.Next() {
		t.Fatal("Next must return false after cancellation")
	}
	if !errors.Is(c.Err(), context.Canceled) {
		t.Fatalf("err=%v, want context.Canceled", c.Err())
	}
	if len(opener.calls) != 0 {
		t.Fatalf("opener must not be called once cancelled, calls=%v", opener.calls)
	}
}

func TestObserverEmitsStructuredAttemptEvents(t *testing.T) {
	var events []AttemptEvent
	opener := &scriptedOpener{
		responses: []scriptedResponse{
			{body: sse("2", `{"v":"b"}`) + "data: [DONE]\n\n"},
		},
	}
	first := sse("1", `{"v":"a"}`)
	c := newTestController(t, Config{
		MaxAttempts: 2,
		OnAttempt: ObserverFunc(func(e AttemptEvent) {
			events = append(events, e)
		}),
	}, opener, first)

	_ = drain(t, c)
	if len(events) != 1 {
		t.Fatalf("events=%d, want 1", len(events))
	}
	e := events[0]
	if e.Attempt != 1 || !e.Resume || e.Position != "1" || e.Reason != ReasonConnectionEnd {
		t.Fatalf("event=%#v", e)
	}
	if e.Waited <= 0 {
		t.Fatal("waited should be recorded")
	}
}

func TestPositionFieldFromJSON(t *testing.T) {
	opener := &scriptedOpener{
		responses: []scriptedResponse{
			{body: "data: {\"seq\":9,\"v\":\"b\"}\n\ndata: [DONE]\n\n"},
		},
	}
	first := "data: {\"seq\":8,\"v\":\"a\"}\n\n"
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader(first)),
	}
	c := NewController(context.Background(), Config{MaxAttempts: 2, PositionField: "seq"},
		opener, resp, ssestream.NewDecoder(resp), newPlainDecoder)
	c.sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	c.backoff = func(_ int, _ time.Duration) time.Duration { return time.Millisecond }

	got := drain(t, c)
	if err := c.Err(); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("events=%d, want 2", len(got))
	}
	if len(opener.calls) != 1 || opener.calls[0].position != "8" {
		t.Fatalf("calls=%v, want renewal at JSON position 8", opener.calls)
	}
}

func TestUnparseableFrameTerminatesWithoutRecovery(t *testing.T) {
	// A decoder that reports bufio.ErrTooLong is a deterministic framing
	// failure: the controller must not spend the retry budget on it.
	opener := &scriptedOpener{
		responses: []scriptedResponse{{body: "data: [DONE]\n\n"}},
	}
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"text/event-stream"}},
		Body:       io.NopCloser(strings.NewReader("")),
	}
	c := NewController(context.Background(), Config{MaxAttempts: 3}, opener, resp,
		&framingDecoder{err: bufio.ErrTooLong}, newPlainDecoder)
	c.sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }

	if c.Next() {
		t.Fatal("framing failure must end iteration")
	}
	var term *TerminalError
	if !errors.As(c.Err(), &term) || term.Code != ErrCodeUnparseableEvent {
		t.Fatalf("err=%v, want unparseable terminal", c.Err())
	}
	if len(opener.calls) != 0 {
		t.Fatalf("framing failure must not trigger recovery, calls=%v", opener.calls)
	}
}

type framingDecoder struct{ err error }

func (d *framingDecoder) Next() bool             { return false }
func (d *framingDecoder) Event() ssestream.Event { return ssestream.Event{} }
func (d *framingDecoder) Close() error           { return nil }
func (d *framingDecoder) Err() error             { return d.err }

func TestTerminalErrorSatisfiesSentinels(t *testing.T) {
	cases := []struct {
		code     string
		sentinel error
	}{
		{ErrCodeGap, ErrGap},
		{ErrCodeNoResume, ErrNoResume},
		{ErrCodeBudgetExhausted, ErrBudgetExhausted},
		{ErrCodeNonRetryableStatus, ErrNonRetryableStatus},
		{ErrCodeUnparseableEvent, ErrUnparseableEvent},
	}
	for _, tc := range cases {
		term := newTerminal(tc.code, "x")
		if !errors.Is(term, tc.sentinel) {
			t.Errorf("code %s does not satisfy %v", tc.code, tc.sentinel)
		}
	}
	if errors.Is(newTerminal(ErrCodeGap, "x"), ErrNoResume) {
		t.Error("distinct codes must not satisfy the wrong sentinel")
	}
}

func TestBackoffIsBoundedByMaxDelay(t *testing.T) {
	cases := []struct {
		retry    int
		maxDelay time.Duration
	}{
		{0, time.Second},
		{1, time.Second},
		{10, 100 * time.Millisecond},
	}
	for _, tc := range cases {
		// Run many iterations to tolerate random jitter.
		for range 50 {
			d := computeBackoff(tc.retry, tc.maxDelay)
			if d <= 0 || d > tc.maxDelay {
				t.Fatalf("backoff(%d,%v)=%v out of bounds", tc.retry, tc.maxDelay, d)
			}
		}
	}
}
