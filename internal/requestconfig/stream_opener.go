package requestconfig

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/openai/openai-go/v3/internal/streamconfig"
	"github.com/openai/openai-go/v3/internal/streamrecovery"
)

var errNonReplayableStreamBody = errors.New("requestconfig: stream recovery requires a replayable request body")

// streamOpener re-dispatches one logical streaming request for recovery. Each
// Open call builds a fresh configuration clone and a fresh *http.Response; a
// previous response object is never reused. Authentication, headers,
// timeouts, cancellation, and middleware all run again on every attempt.
type streamOpener struct {
	cfg          *RequestConfig
	resumeHeader string
}

func newStreamOpener(cfg *RequestConfig, resumeHeader string) *streamOpener {
	return &streamOpener{cfg: cfg, resumeHeader: resumeHeader}
}

// NewStreamOpener constructs the recovery opener for one logical streaming
// request. This is internal API used by the stream wiring layer.
func NewStreamOpener(cfg *RequestConfig, resumeHeader string) streamrecovery.Opener {
	return newStreamOpener(cfg, resumeHeader)
}

// Open implements streamrecovery.Opener. An empty position replays the
// original request in full; a non-empty position renews the subscription.
// The transport's own retry loop stays disabled for recovery attempts: the
// single recovery budget owned by the controller decides retries, so network
// failures and retryable statuses return directly to it.
func (o *streamOpener) Open(ctx context.Context, position string) (*http.Response, error) {
	// The first Execute derived GetBody from cfg.Body and consumed the
	// original reader. Re-derive a replayable body from the serialized
	// payload before cloning so every renewal resends the identical request
	// body without reusing any prior response object.
	if err := o.cfg.ensureReplayableBody(); err != nil {
		return nil, WithNoRetryError(err)
	}

	// Renew on the caller's root context rather than a failed connection's
	// request-scoped context. The controller passes that same context, which
	// still observes caller cancellation.
	rootCtx := o.cfg.rootContext
	if rootCtx == nil {
		rootCtx = ctx
	}
	clone := o.cfg.Clone(rootCtx)
	if clone.cloneError != nil {
		return nil, WithNoRetryError(clone.cloneError)
	}
	clone.MaxRetries = 0
	clone.streamRawErrorResponse = true
	// The cloned config still points at the original call's *http.Response
	// destination. Redirect both response targets at this attempt's local so
	// a renewal result is never written into the initial Execute's variable.
	var raw *http.Response
	clone.ResponseInto = &raw
	clone.ResponseBodyInto = &raw
	// Re-attach the recovery configuration to the fresh request context so a
	// response produced by this attempt decodes through the same controller.
	if clone.StreamRecovery.Enabled {
		clone.Request = clone.Request.WithContext(
			streamconfig.WithContext(clone.Request.Context(), clone.StreamRecovery))
	}

	// Attach the resume point using the upstream's header convention. A full
	// resend (empty position) sends no such header.
	if position != "" {
		clone.SetHeader(o.resumeHeader, position)
	}

	if err := clone.Execute(); err != nil {
		// Execute populates raw even on some error paths; ownership of that
		// response is handed to the controller, which closes it.
		return raw, err
	}
	return raw, nil
}

// ensureReplayableBody restores a GetBody function after the initial Execute
// consumed the request reader, using the same body materialization rules as
// Execute. Bodies that were never replayable remain non-replayable and the
// controller's full-resend boundary rejects them deterministically.
func (cfg *RequestConfig) ensureReplayableBody() error {
	req := cfg.Request
	if req.Body == nil || req.Body == http.NoBody || req.GetBody != nil {
		return nil
	}
	switch body := cfg.Body.(type) {
	case *bytes.Buffer:
		b := body.Bytes()
		req.ContentLength = int64(body.Len())
		req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil }
	case *bytes.Reader:
		req.GetBody = func() (io.ReadCloser, error) {
			if _, err := body.Seek(0, 0); err != nil {
				return nil, err
			}
			return io.NopCloser(body), nil
		}
	default:
		// A streaming/non-seekable body cannot be replayed; signal a
		// deterministic, non-retryable failure to the controller.
		return WithNoRetryError(errNonReplayableStreamBody)
	}
	return nil
}

// Compile-time assertion that the opener satisfies the recovery contract.
var _ streamrecovery.Opener = (*streamOpener)(nil)
