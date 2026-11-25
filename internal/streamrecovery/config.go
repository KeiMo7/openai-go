// Package streamrecovery implements the opt-in recovery semantics for HTTP
// Server-Sent-Events streams. All state lives on a single Controller created
// for one logical stream; the package keeps no mutable package-level state, so
// concurrent streams never share budgets or resume positions.
package streamrecovery

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// Reason classifies why a recovery attempt was started. Values are stable and
// never carry fragment bodies, credentials, or full URLs.
type Reason string

const (
	// ReasonConnectionEnd means the transport ended without the upstream
	// terminal marker while no HTTP status was available.
	ReasonConnectionEnd Reason = "connection_end"
	// ReasonStatus means the upstream answered a reconnect attempt with a
	// retryable HTTP status code.
	ReasonStatus Reason = "retryable_status"
	// ReasonTransport means the reconnect attempt failed at the transport
	// layer (DNS, connection, reset, ...).
	ReasonTransport Reason = "transport_error"
)

// TerminalError ends recovery immediately. It preserves the fragments already
// delivered and explains why the partial result cannot continue.
type TerminalError struct {
	// Code is one of the stable reason constants below.
	Code string
	// GapSize reports how many sequence positions were skipped when Code is
	// ErrCodeGap. It is zero for every other code.
	GapSize int
	message string
}

// Stable terminal reason codes.
const (
	// ErrCodeNoResume is returned after fragments were delivered when the
	// upstream provides no resumption position, so a full resend is forbidden.
	ErrCodeNoResume = "resume_position_unavailable"
	// ErrCodeGap is returned when a reconnect resumes after a position beyond
	// the last delivered one, leaving an unrecoverable hole.
	ErrCodeGap = "resume_gap"
	// ErrCodePositionMovedBackward is reserved for an incompatible position
	// ordering. Numeric backward moves are reliably detected and discarded as
	// duplicates (ErrGap is only used for forward skips); opaque, non-numeric
	// positions carry no ordering, so a distinct opaque value cannot be proven
	// older and is delivered rather than discarded.
	ErrCodePositionMovedBackward = "resume_position_incompatible"
	// ErrCodeBudgetExhausted is returned when the shared retry budget has
	// been consumed; the partial result stays with the caller.
	ErrCodeBudgetExhausted = "recovery_budget_exhausted"
	// ErrCodeNonRetryableStatus is returned when a reconnect receives an HTTP
	// status that must not be retried.
	ErrCodeNonRetryableStatus = "non_retryable_status"
	// ErrCodeUnparseableEvent is returned when an SSE frame cannot be decoded
	// (for example a line exceeding the scanner's token limit). A framing
	// failure is deterministic and must not be retried as a network blip.
	ErrCodeUnparseableEvent = "stream_event_unparseable"
)

func (e *TerminalError) Error() string {
	if e == nil {
		return "streamrecovery: terminal error"
	}
	return "streamrecovery: " + e.message
}

func newTerminal(code, message string) *TerminalError {
	return &TerminalError{Code: code, message: message}
}

var (
	// ErrGap marks a skipped sequence position between the delivered prefix
	// and the first event of a renewed connection.
	ErrGap = errors.New("streamrecovery: gap in resumed stream")
	// ErrNoResume marks that no resumption position was provided after at
	// least one fragment had already been delivered.
	ErrNoResume = errors.New("streamrecovery: no resumption position after delivered fragments")
	// ErrBudgetExhausted marks that the single retry budget was consumed.
	ErrBudgetExhausted = errors.New("streamrecovery: retry budget exhausted")
	// ErrNonRetryableStatus marks that a reconnect returned a status the
	// recovery rules forbid treating as a transient network failure.
	ErrNonRetryableStatus = errors.New("streamrecovery: non-retryable reconnect status")
	// ErrUnparseableEvent marks an SSE frame that could not be decoded.
	ErrUnparseableEvent = errors.New("streamrecovery: stream event could not be parsed")
)

// terminalSentinels maps each stable reason code to its errors.Is target.
var terminalSentinels = map[string]error{
	ErrCodeGap:                   ErrGap,
	ErrCodeNoResume:              ErrNoResume,
	ErrCodeBudgetExhausted:       ErrBudgetExhausted,
	ErrCodeNonRetryableStatus:    ErrNonRetryableStatus,
	ErrCodeUnparseableEvent:      ErrUnparseableEvent,
	ErrCodePositionMovedBackward: ErrGap,
}

// Is lets callers use errors.Is(terminalErr, ErrGap) and similar against the
// stable reason-code sentinels, in addition to inspecting TerminalError.Code.
func (e *TerminalError) Is(target error) bool {
	if e == nil {
		return false
	}
	if target == nil {
		return false
	}
	sentinel, ok := terminalSentinels[e.Code]
	return ok && target == sentinel
}

// Opener opens one physical stream connection. A nil position replays the
// original request in full; a non-nil position renews the subscription at the
// upstream resume point. Implementations must build a fresh authenticated
// request, never reuse a previous response object. The returned *http.Response
// is owned by the caller, which closes its body.
type Opener interface {
	Open(ctx context.Context, position string) (*http.Response, error)
}

// OpenerFunc adapts a function to the Opener interface.
type OpenerFunc func(ctx context.Context, position string) (*http.Response, error)

// Open implements Opener.
func (f OpenerFunc) Open(ctx context.Context, position string) (*http.Response, error) {
	return f(ctx, position)
}

// AttemptEvent is the structured observation emitted once per recovery
// attempt. It contains positions and counters only: never fragment payloads,
// credentials, or the full request URL.
type AttemptEvent struct {
	// Attempt is the one-based ordinal of the recovery attempt; the initial
	// connection is attempt zero and emits no event.
	Attempt int
	// Reason is why the attempt was started.
	Reason Reason
	// Waited is the backoff (with jitter, bounded by the configured maximum)
	// spent before the attempt.
	Waited time.Duration
	// Resume is true when the attempt carried a resumption position, false
	// for a full resend before the first fragment was delivered.
	Resume bool
	// Position is the resume point sent on a renewal, empty for a full resend.
	Position string
	// DuplicateFragments is the number of re-sent fragments discarded by
	// this attempt so far at the time the event was emitted. Events are
	// emitted before the attempt starts reading, so this counts fragments
	// discarded by earlier attempts; the final Stats value is authoritative.
	DuplicateFragments int
	// BudgetExhausted reports whether this attempt consumed the last unit of
	// the shared budget.
	BudgetExhausted bool
}

// Stats holds the cumulative counters for one logical stream.
type Stats struct {
	// Attempts is the number of recovery attempts after the initial
	// connection.
	Attempts int
	// FullResends is the number of attempts made before any fragment was
	// delivered, replaying the original request in full.
	FullResends int
	// Renewals is the number of attempts that carried a resume position.
	Renewals int
	// DuplicateFragments is the total number of re-sent fragments discarded
	// across every renewal.
	DuplicateFragments int
	// Gaps is the total number of skipped sequence positions detected.
	Gaps int
}

// Observer receives one AttemptEvent per recovery attempt and a final Stats
// snapshot. Implementations must not retain or log fragment data; the event
// never contains any.
type Observer interface {
	OnRecoveryAttempt(AttemptEvent)
}

// ObserverFunc adapts a function to the Observer interface.
type ObserverFunc func(AttemptEvent)

// OnRecoveryAttempt implements Observer.
func (f ObserverFunc) OnRecoveryAttempt(event AttemptEvent) {
	if f != nil {
		f(event)
	}
}

// Config configures recovery for one logical stream.
type Config struct {
	// MaxAttempts bounds recovery attempts (full resends and renewals share
	// this single budget). Zero selects a default of two, matching the
	// SDK's ordinary retry default.
	MaxAttempts int
	// MaxDelay bounds every backoff wait, including upstream-suggested ones.
	// Zero selects the SDK default of eight seconds.
	MaxDelay time.Duration
	// PositionField optionally names a JSON field inside each event's data
	// that carries the resumption position. When empty the SSE "id:" field is
	// used.
	PositionField string
	// ResumeHeader names the request header used to send the resume position
	// to the upstream. Empty selects "Last-Event-ID", the SSE convention.
	ResumeHeader string
	// OnAttempt observes each recovery attempt.
	OnAttempt Observer
}

// Default configuration values.
const (
	defaultMaxAttempts = 2
	defaultMaxDelay    = 8 * time.Second
	// DefaultResumeHeader is the Server-Sent Events convention for renewals.
	DefaultResumeHeader = "Last-Event-ID"
)

func (c Config) withDefaults() Config {
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = defaultMaxAttempts
	}
	if c.MaxDelay <= 0 {
		c.MaxDelay = defaultMaxDelay
	}
	if c.ResumeHeader == "" {
		c.ResumeHeader = DefaultResumeHeader
	}
	return c
}
