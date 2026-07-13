package opt

import (
	"context"
	"time"
)

type Opts struct {
	Ctx         context.Context
	NonBlocking bool
	Timeout     time.Duration
}

type Option func(*Opts)

// WithContext applies a context for cancellation in blocking operations.
func WithContext(ctx context.Context) Option {
	return func(o *Opts) {
		o.Ctx = ctx
	}
}

// NonBlocking instructs an operation (like Send or Receive) to not block.
func NonBlocking() Option {
	return func(o *Opts) {
		o.NonBlocking = true
	}
}

// WithTimeout specifies a duration for operations like Batch.
func WithTimeout(d time.Duration) Option {
	return func(o *Opts) {
		o.Timeout = d
	}
}

func ApplyOpts(opts []Option) Opts {
	var o Opts
	for _, opt := range opts {
		opt(&o)
	}
	if o.Ctx == nil {
		o.Ctx = context.Background()
	}
	return o
}
