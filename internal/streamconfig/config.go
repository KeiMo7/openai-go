// Package streamconfig carries the opt-in stream recovery configuration
// between the public option layer, the request machinery, and the SSE package
// without introducing an import cycle. It holds no mutable package-level
// state: every value travels with one logical request.
package streamconfig

import (
	"context"
	"time"
)

// Observer receives structured recovery events. The event never contains
// fragment payloads, credentials, or full URLs, only positions and counters.
type Observer func(Event)

// Event is the public, serializable observation emitted once per recovery
// attempt.
type Event struct {
	// Attempt is the one-based ordinal of the recovery attempt; the initial
	// connection is attempt zero and emits no event.
	Attempt int `json:"attempt"`
	// Reason classifies why the attempt started.
	Reason string `json:"reason"`
	// Waited is the bounded backoff with jitter spent before the attempt.
	Waited time.Duration `json:"waited"`
	// Resume is true when the attempt carried a resumption position.
	Resume bool `json:"resume"`
	// Position is the resume point (empty for a full pre-delivery resend).
	Position string `json:"position,omitempty"`
	// DuplicateFragments counts fragments re-sent by the upstream and
	// discarded by earlier attempts.
	DuplicateFragments int `json:"duplicate_fragments"`
	// BudgetExhausted reports whether the attempt consumed the last unit of
	// the single shared budget.
	BudgetExhausted bool `json:"budget_exhausted"`
}

// Stats is the cumulative counter snapshot for one logical stream.
type Stats struct {
	Attempts           int `json:"attempts"`
	FullResends        int `json:"full_resends"`
	Renewals           int `json:"renewals"`
	DuplicateFragments int `json:"duplicate_fragments"`
	Gaps               int `json:"gaps"`
}

// Config is the public recovery configuration attached to a request. The
// zero value disables recovery; construction goes through WithStreamRecovery.
type Config struct {
	Enabled       bool
	MaxAttempts   int
	MaxDelay      time.Duration
	PositionField string
	ResumeHeader  string
	OnEvent       Observer
}

type contextKey struct{}

// WithContext returns a context carrying the recovery configuration.
func WithContext(ctx context.Context, cfg Config) context.Context {
	if !cfg.Enabled {
		return ctx
	}
	return context.WithValue(ctx, contextKey{}, &cfg)
}

// FromContext returns the recovery configuration attached to ctx, if any.
func FromContext(ctx context.Context) (*Config, bool) {
	cfg, ok := ctx.Value(contextKey{}).(*Config)
	return cfg, ok
}
