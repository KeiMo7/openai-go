// Package streamwire connects the opt-in stream recovery configuration to the
// SSE decoder. It is the single place allowed to know about both the internal
// streamrecovery controller and the public ssestream registry; importing it
// once from the root package installs the factory. There is no mutable
// package-level stream state here: registration only installs a constructor.
package streamwire

import (
	"context"
	"net/http"

	"github.com/openai/openai-go/v3/internal/requestconfig"
	"github.com/openai/openai-go/v3/internal/streamconfig"
	"github.com/openai/openai-go/v3/internal/streamrecovery"
	"github.com/openai/openai-go/v3/packages/ssestream"
)

// decoder adapts *streamrecovery.Controller to ssestream.Decoder.
type decoder struct {
	*streamrecovery.Controller
}

func (d *decoder) resumableMarker() {}

// Stats exposes the cumulative counters through the narrow surface used by
// tests and callers that opt into recovery.
func (d *decoder) Stats() streamconfig.Stats {
	s := d.Controller.Stats()
	return streamconfig.Stats{
		Attempts:           s.Attempts,
		FullResends:        s.FullResends,
		Renewals:           s.Renewals,
		DuplicateFragments: s.DuplicateFragments,
		Gaps:               s.Gaps,
	}
}

// Install registers the recovery factory with the ssestream package. The
// root package blank-imports this package, whose init performs the one-time
// registration. There is no per-stream package-level state: registration only
// installs a constructor.
func Install() {
	ssestream.RegisterResumableFactory(newResumableDecoder)
}

func init() {
	Install()
}

func newResumableDecoder(
	ctx context.Context,
	initialResponse *http.Response,
	initial ssestream.Decoder,
	wrap func(*http.Response) ssestream.Decoder,
) (ssestream.Decoder, bool) {
	cfg, ok := streamconfig.FromContext(ctx)
	if !ok || cfg == nil || !cfg.Enabled {
		return nil, false
	}

	// Recover the request configuration placed on this request by the option
	// layer. It is the template for every renewal clone.
	rc := requestconfig.RequestConfigFromContext(ctx)
	if rc == nil {
		return nil, false
	}

	recoveryCfg := streamrecovery.Config{
		MaxAttempts:   cfg.MaxAttempts,
		MaxDelay:      cfg.MaxDelay,
		PositionField: cfg.PositionField,
		ResumeHeader:  cfg.ResumeHeader,
	}
	if cfg.OnEvent != nil {
		recoveryCfg.OnAttempt = streamrecovery.ObserverFunc(func(event streamrecovery.AttemptEvent) {
			cfg.OnEvent(streamconfig.Event{
				Attempt:            event.Attempt,
				Reason:             string(event.Reason),
				Waited:             event.Waited,
				Resume:             event.Resume,
				Position:           event.Position,
				DuplicateFragments: event.DuplicateFragments,
				BudgetExhausted:    event.BudgetExhausted,
			})
		})
	}

	opener := requestconfig.NewStreamOpener(rc, resumeHeader(cfg))
	controller := streamrecovery.NewController(ctx, recoveryCfg, opener, initialResponse, initial,
		func(resp *http.Response) streamrecovery.Decoder {
			return wrap(resp)
		})
	return &decoder{Controller: controller}, true
}

func resumeHeader(cfg *streamconfig.Config) string {
	if cfg.ResumeHeader != "" {
		return cfg.ResumeHeader
	}
	return streamrecovery.DefaultResumeHeader
}
