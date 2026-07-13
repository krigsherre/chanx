package opt

import "errors"

// ErrClosed is returned when an operation targets a closed channel.
var ErrClosed = errors.New("chanx: channel closed")
