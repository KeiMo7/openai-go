package option

import (
	"time"

	"github.com/openai/openai-go/v3/internal/requestconfig"
	"github.com/openai/openai-go/v3/internal/streamconfig"
)

// StreamRecoveryConfig configures opt-in recovery and retry semantics for an
// HTTP SSE stream. Recovery is disabled unless Enabled is true; when disabled
// the stream's delivery cadence, error types, and termination behavior are
// byte-for-byte unchanged.
//
// Recovery boundaries:
//   - If the stream fails before the first fragment is delivered, the original
//     request is replayed in full and the attempt counts against the single
//     retry budget.
//   - After any fragment has been delivered, a full replay is forbidden. The
//     stream is renewed only at an upstream-provided resume position. Without
//     a position, the partial result is returned with a non-resumable error.
//
// One budget is shared by full resends and renewals, and by any additional
// provider authentication steps on the same logical stream. State lives on
// the individual stream; there is no package-level mutable state, so
// concurrent streams never interfere with each other.
type StreamRecoveryConfig struct {
	// Enabled turns recovery on. It defaults to false.
	Enabled bool
	// MaxAttempts bounds recovery attempts after the initial connection. Zero
	// selects the SDK default of two. Full resends and renewals share it.
	MaxAttempts int
	// MaxDelay bounds every backoff wait, including any upstream-suggested
	// wait. Zero selects the SDK default of eight seconds.
	MaxDelay time.Duration
	// PositionField optionally names a JSON field inside each event whose
	// value is the resume position. When empty, the SSE "id:" field is used.
	PositionField string
	// ResumeHeader names the request header carrying the resume position on a
	// renewal. Empty selects "Last-Event-ID", the Server-Sent Events
	// convention.
	ResumeHeader string
	// OnEvent, when non-nil, receives one structured event per recovery
	// attempt. Events contain positions and counters only — never fragment
	// payloads, credentials, or full URLs.
	OnEvent func(streamconfig.Event)
}

// WithStreamRecovery attaches optional stream recovery semantics to every
// streaming request produced with this option. It is purely additive: passing
// a zero-value or disabled configuration leaves all existing behavior
// unchanged. Non-streaming requests ignore it. The option may be applied at the
// client, service, or call level like any other RequestOption.
func WithStreamRecovery(config StreamRecoveryConfig) RequestOption {
	cfg := streamconfig.Config{
		Enabled:       config.Enabled,
		MaxAttempts:   config.MaxAttempts,
		MaxDelay:      config.MaxDelay,
		PositionField: config.PositionField,
		ResumeHeader:  config.ResumeHeader,
		OnEvent:       config.OnEvent,
	}
	return requestconfig.RequestOptionFunc(func(r *requestconfig.RequestConfig) error {
		r.StreamRecovery = cfg
		return nil
	})
}
