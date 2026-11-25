package openai

import (
	// The wire package installs the opt-in SSE recovery factory into the
	// ssestream registry. The import is side-effect only; every per-stream
	// state still lives on the individual stream, never at package level.
	_ "github.com/openai/openai-go/v3/internal/streamwire"
)
