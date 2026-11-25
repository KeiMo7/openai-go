package requestconfig

import "context"

type requestConfigContextKey struct{}

// WithRequestConfig associates the logical request's configuration with ctx.
// The streaming recovery opener retrieves it when building a renewal clone.
// This is internal API and may change without notice.
func WithRequestConfig(ctx context.Context, cfg *RequestConfig) context.Context {
	if cfg == nil {
		return ctx
	}
	return context.WithValue(ctx, requestConfigContextKey{}, cfg)
}

// RequestConfigFromContext returns the request configuration attached to ctx,
// if any.
func RequestConfigFromContext(ctx context.Context) *RequestConfig {
	cfg, _ := ctx.Value(requestConfigContextKey{}).(*RequestConfig)
	return cfg
}
