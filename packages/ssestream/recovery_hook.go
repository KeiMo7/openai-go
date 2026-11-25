package ssestream

import (
	"context"
	"net/http"
	"sync"
)

// ResumableDecoder marks a decoder that already owns recovery, exposing its
// cumulative stats. The concrete recovery decoder implements it.
type ResumableDecoder interface {
	Decoder
	resumableMarker()
}

// ResumableFactory builds a resumable decoder for one logical stream given
// the request context, the first physical response and its standard decoder,
// and a way to wrap subsequent responses. When no factory is registered,
// streaming behaves exactly as it did before recovery existed.
type ResumableFactory func(
	ctx context.Context,
	initialResponse *http.Response,
	initial Decoder,
	wrap func(*http.Response) Decoder,
) (Decoder, bool)

var (
	resumableMu      sync.RWMutex
	resumableFactory ResumableFactory
)

// RegisterResumableFactory installs the internal recovery implementation. It is
// called once from an internal wiring package and is not part of the public
// API surface.
func RegisterResumableFactory(factory ResumableFactory) {
	resumableMu.Lock()
	defer resumableMu.Unlock()
	resumableFactory = factory
}

func resumableFactoryFromRegistry() ResumableFactory {
	resumableMu.RLock()
	defer resumableMu.RUnlock()
	return resumableFactory
}
