package streamrecovery

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/openai/openai-go/v3/packages/ssestream"
	"github.com/tidwall/gjson"
)

// isFramingError reports whether err describes an SSE framing/parse failure
// rather than a transport-level interruption.
func isFramingError(err error) bool {
	return errors.Is(err, bufio.ErrTooLong)
}

// Decoder is the SSE decoder surface the controller drives. It matches
// ssestream.Decoder so the standard text/event-stream decoder plugs in
// directly.
type Decoder interface {
	Next() bool
	Event() ssestream.Event
	Close() error
	Err() error
}

// decoderFactory builds a Decoder for one physical response.
type decoderFactory func(*http.Response) Decoder

// Controller owns the recovery state for exactly one logical stream. The zero
// value is not usable; construct it with NewController. A Controller and its
// decoder are driven by a single goroutine, matching ssestream's iteration
// contract; Close may be called concurrently.
type Controller struct {
	opener Opener
	newDec decoderFactory
	cfg    Config
	ctx    context.Context

	dec       Decoder
	resp      *http.Response
	attempt   int
	delivered int
	lastPos   position
	havePos   bool
	stats     Stats
	terminal  error
	// done is set only when the upstream terminal marker was observed.
	done atomic.Bool
	// closed makes Close idempotent and safe under concurrent calls.
	closed   atomic.Bool
	closeMu  sync.Mutex
	closeErr error

	// sleep and backoff are indirected for deterministic tests.
	sleep   func(context.Context, time.Duration) error
	backoff func(retryCount int, maxDelay time.Duration) time.Duration
}

// NewController creates the recovery controller for one logical stream. The
// opener re-dispatches the original request; newDecoder wraps each fresh
// response's body. initialResponse/initialDecoder are the result of the first
// dispatch, whose transport-level retry behavior is owned by the request
// layer and is not counted in the recovery budget.
func NewController(ctx context.Context, cfg Config, opener Opener, initialResponse *http.Response, initialDecoder Decoder, newDecoder decoderFactory) *Controller {
	cfg = cfg.withDefaults()
	c := &Controller{
		opener:  opener,
		newDec:  newDecoder,
		cfg:     cfg,
		ctx:     ctx,
		resp:    initialResponse,
		dec:     initialDecoder,
		sleep:   waitForDelay,
		backoff: computeBackoff,
	}
	return c
}

// Stats returns a point-in-time copy of the cumulative counters.
func (c *Controller) Stats() Stats {
	return c.stats
}

// position is an opaque resume point with an optional numeric ordering. Numeric
// ordering enables gap and backward-move detection; when the upstream positions
// are not numeric, equality still provides overlap deduplication.
type position struct {
	raw     string
	seq     int64
	numeric bool
}

func parsePosition(raw string) position {
	p := position{raw: raw}
	if raw == "" {
		return p
	}
	if seq, err := strconv.ParseInt(raw, 10, 64); err == nil {
		p.seq = seq
		p.numeric = true
	}
	return p
}

// compare returns -1/0/1 when both positions are numeric and ordered, and a
// second result of false when ordering is unavailable (non-numeric positions).
func (p position) compare(next position) (int, bool) {
	if !p.numeric || !next.numeric {
		return 0, false
	}
	switch {
	case next.seq < p.seq:
		return -1, true
	case next.seq > p.seq:
		return 1, true
	default:
		return 0, true
	}
}

// extractPosition reads the resume point carried by an event. The SSE "id:"
// field is the default; a configured JSON field in the event data overrides
// it. Only the position string is retained, never the event body.
func (c *Controller) extractPosition(event ssestream.Event) string {
	if c.cfg.PositionField != "" {
		if result := gjson.GetBytes(event.Data, c.cfg.PositionField); result.Exists() && result.Type != gjson.Null {
			return result.String()
		}
	}
	return event.ID
}

// doneMarker is stored as terminal when the upstream [DONE] marker is seen.
type doneMarker struct{}

func (doneMarker) Error() string { return "streamrecovery: terminal marker received" }

// Done reports whether the stream ended on the upstream terminal marker. It is
// internal; the wiring layer uses it to map a clean end onto a nil stream
// error.
func (c *Controller) Done() bool { return c.done.Load() }

// DoneMarker reports whether err is the clean terminal marker. It is internal.
func DoneMarker(err error) bool {
	_, ok := err.(doneMarker)
	return ok
}

// terminalEventData marks the upstream [DONE] terminal event.
var terminalEventData = []byte("[DONE]")

// Next drives iteration, transparently performing recovery underneath. It
// returns true only for fragments the caller has not seen. On termination it
// returns false; call Err to distinguish a clean terminal marker from a
// recovery failure.
func (c *Controller) Next() bool {
	if c.terminal != nil || c.closed.Load() {
		return false
	}
	for {
		if c.ctx.Err() != nil {
			return c.fail(c.ctx.Err())
		}
		if c.dec == nil {
			return false
		}
		if !c.dec.Next() {
			decErr := c.dec.Err()
			// A framing/parse failure (e.g. a line past the scanner token
			// limit) is deterministic: replaying the identical stream cannot
			// fix it, so it terminates with an explicit unparseable cause
			// instead of being retried as a network interruption.
			if isFramingError(decErr) {
				return c.fail(&TerminalError{
					Code:    ErrCodeUnparseableEvent,
					message: "stream event could not be parsed: " + decErr.Error(),
				})
			}
			// A plain decoder end without a terminal marker is an early
			// connection close under recovery semantics; attempt recovery.
			if err := c.handleEnd(decErr); err != nil {
				return c.fail(err)
			}
			continue
		}

		event := c.dec.Event()

		// Only the upstream terminal marker counts as a clean end. An early
		// connection close never reaches this branch and enters recovery
		// instead, eliminating silent truncation.
		if bytes.HasPrefix(event.Data, terminalEventData) {
			c.terminal = doneMarker{}
			c.done.Store(true)
			_ = c.Close()
			return false
		}

		posRaw := c.extractPosition(event)
		pos := parsePosition(posRaw)

		if c.havePos && posRaw != "" {
			if posRaw == c.lastPos.raw {
				// Exact overlap (numeric or opaque): already delivered.
				c.stats.DuplicateFragments++
				continue
			}
			if rel, ordered := c.lastPos.compare(pos); ordered {
				if rel < 0 {
					// Numeric backward move to a different, older position.
					c.stats.DuplicateFragments++
					continue
				}
				if pos.seq > c.lastPos.seq+1 {
					gap := int(pos.seq - c.lastPos.seq - 1)
					c.stats.Gaps += gap
					return c.fail(&TerminalError{
						Code:    ErrCodeGap,
						GapSize: gap,
						message: "resumed stream skipped " + strconv.Itoa(gap) + " position(s)",
					})
				}
			}
			// Opaque, different positions: no ordering is available, so the
			// fragment cannot be proven duplicate; deliver it. Equality was
			// handled above.
		}

		if posRaw != "" {
			c.lastPos = pos
			c.havePos = true
		}
		c.delivered++
		return true
	}
}

// Event returns the current decoded event.
func (c *Controller) Event() ssestream.Event {
	return c.dec.Event()
}

// Err returns the first terminal error, or nil when the stream ended on the
// upstream terminal marker (see Done) or has not terminated yet.
func (c *Controller) Err() error {
	if c.done.Load() {
		return nil
	}
	return c.terminal
}

// Close releases the active physical connection. It is safe to call
// concurrently and repeatedly; the first close result is preserved.
func (c *Controller) Close() error {
	if !c.closed.CompareAndSwap(false, true) {
		c.closeMu.Lock()
		err := c.closeErr
		c.closeMu.Unlock()
		return err
	}
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	if c.dec != nil {
		c.closeErr = c.dec.Close()
		return c.closeErr
	}
	if c.resp != nil && c.resp.Body != nil {
		c.closeErr = c.resp.Body.Close()
		return c.closeErr
	}
	return nil
}

func (c *Controller) fail(err error) bool {
	c.terminal = err
	_ = c.Close()
	return false
}

// handleEnd reacts to a physical connection ending without a terminal
// marker, deciding between a full resend, a renewal, or a terminal partial
// result.
func (c *Controller) handleEnd(decErr error) error {
	_ = c.closeActive()

	// A bare EOF is the ordinary shape of an abruptly closed SSE connection;
	// other scanner errors describe the same interruption. Either way the
	// recovery rules, not the decoder error text, decide the outcome.
	_ = decErr

	// Cancellation wins over every recovery rule.
	if err := c.ctx.Err(); err != nil {
		return err
	}

	if c.delivered == 0 {
		// Before the first fragment a full replay of the original request is
		// the only legal move and consumes the shared budget.
		return c.runRecovery("", false, ReasonConnectionEnd)
	}

	if !c.havePos {
		// Fragments were delivered but the upstream gave no resumption
		// point. A full resend would duplicate delivered fragments, so the
		// partial result is returned with an explicit non-resumable cause.
		return newTerminal(ErrCodeNoResume,
			"stream ended after "+strconv.Itoa(c.delivered)+" fragment(s) without a resumption position")
	}

	return c.runRecovery(c.lastPos.raw, true, ReasonConnectionEnd)
}

// runRecovery performs budgeted recovery attempts, including bounded backoff
// waits, until a fresh decoder is installed or a terminal condition is
// reached. An empty position means a full resend; a non-empty position renews
// the subscription.
func (c *Controller) runRecovery(initialPosition string, initialResume bool, firstReason Reason) error {
	position := initialPosition
	resume := initialResume
	reason := firstReason

	for {
		if c.attempt >= c.cfg.MaxAttempts {
			return c.budgetExhausted("")
		}

		delay := c.backoff(c.attempt, c.cfg.MaxDelay)
		// Cancellation ends the wait immediately.
		if err := c.sleep(c.ctx, delay); err != nil {
			return err
		}

		c.attempt++
		c.stats.Attempts = c.attempt
		if resume {
			c.stats.Renewals++
		} else {
			c.stats.FullResends++
		}
		budgetExhausted := c.attempt >= c.cfg.MaxAttempts

		if c.cfg.OnAttempt != nil {
			c.cfg.OnAttempt.OnRecoveryAttempt(AttemptEvent{
				Attempt:            c.attempt,
				Reason:             reason,
				Waited:             delay,
				Resume:             resume,
				Position:           position,
				DuplicateFragments: c.stats.DuplicateFragments,
				BudgetExhausted:    budgetExhausted,
			})
		}

		resp, err := c.opener.Open(c.ctx, position)
		if err != nil {
			if ctxErr := c.ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			var noRetry interface{ noRetry() }
			if errors.As(err, &noRetry) {
				return err
			}
			if c.attempt >= c.cfg.MaxAttempts {
				return c.budgetExhausted(err.Error())
			}
			position, resume = c.nextTarget()
			reason = ReasonTransport
			continue
		}
		if resp == nil {
			if c.attempt >= c.cfg.MaxAttempts {
				return c.budgetExhausted("opener returned no response")
			}
			position, resume = c.nextTarget()
			reason = ReasonTransport
			continue
		}

		// A non-retryable status ends recovery immediately while preserving
		// the delivered prefix; it must not be treated as a transient network
		// failure and must not keep backing off.
		if resp.StatusCode >= 400 && !retryableStatus(resp.StatusCode) {
			_ = resp.Body.Close()
			return &TerminalError{
				Code:    ErrCodeNonRetryableStatus,
				message: "reconnect returned non-retryable status " + strconv.Itoa(resp.StatusCode),
			}
		}
		if resp.StatusCode >= 400 {
			_ = resp.Body.Close()
			if c.attempt >= c.cfg.MaxAttempts {
				return c.budgetExhausted("retryable status persisted")
			}
			position, resume = c.nextTarget()
			reason = ReasonStatus
			continue
		}

		c.resp = resp
		c.dec = c.newDec(resp)
		return nil
	}
}

// nextTarget re-evaluates the resend boundary for the next hop from the
// current delivery state.
func (c *Controller) nextTarget() (position string, resume bool) {
	if c.delivered > 0 && c.havePos {
		return c.lastPos.raw, true
	}
	return "", false
}

func (c *Controller) budgetExhausted(detail string) error {
	message := "recovery budget of " + strconv.Itoa(c.cfg.MaxAttempts) +
		" attempt(s) exhausted after " + strconv.Itoa(c.delivered) + " delivered fragment(s)"
	if detail != "" {
		message += ": " + detail
	}
	return newTerminal(ErrCodeBudgetExhausted, message)
}

func (c *Controller) closeActive() error {
	if c.dec != nil {
		err := c.dec.Close()
		c.dec = nil
		return err
	}
	if c.resp != nil && c.resp.Body != nil {
		err := c.resp.Body.Close()
		c.resp = nil
		return err
	}
	return nil
}

// retryableStatus mirrors the request layer's classification: 408, 409, 429
// and every 5xx are transient; everything else is terminal for recovery.
func retryableStatus(status int) bool {
	return status == http.StatusRequestTimeout ||
		status == http.StatusConflict ||
		status == http.StatusTooManyRequests ||
		status >= http.StatusInternalServerError
}

// computeBackoff reproduces the SDK's exponential backoff with jitter, capped
// by maxDelay so an upstream suggestion can never exceed the configured bound.
func computeBackoff(retryCount int, maxDelay time.Duration) time.Duration {
	backoff := 0.5 * float64(time.Second) * float64(int64(1)<<retryCount)
	delay := time.Duration(backoff)
	if backoff >= float64(maxDelay) {
		delay = maxDelay
	}
	if jitterRange := int64(delay / 4); jitterRange > 0 {
		delay -= time.Duration(rand.Int63n(jitterRange))
	}
	return delay
}

// waitForDelay waits for delay or until the context is cancelled.
func waitForDelay(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
