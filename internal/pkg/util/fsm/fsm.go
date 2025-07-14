package fsm

import (
	"context"

	"github.com/looplab/fsm"
)

// EventHandler represents a function that handles FSM events
type EventHandler func(ctx context.Context, e *fsm.Event) error

// WrapEvent wraps an event handler to be compatible with fsm.Callback
func WrapEvent(handler EventHandler) fsm.Callback {
	return func(ctx context.Context, e *fsm.Event) {
		if err := handler(ctx, e); err != nil {
			e.Err = err
		}
	}
}

// WrapEventWithoutError wraps an event handler that doesn't return an error
func WrapEventWithoutError(handler func(ctx context.Context, e *fsm.Event)) fsm.Callback {
	return func(ctx context.Context, e *fsm.Event) {
		handler(ctx, e)
	}
}

// CreateCallback creates a callback function for FSM events
func CreateCallback(handler func(ctx context.Context, e *fsm.Event) error) fsm.Callback {
	return func(ctx context.Context, e *fsm.Event) {
		if err := handler(ctx, e); err != nil {
			e.Err = err
		}
	}
}
