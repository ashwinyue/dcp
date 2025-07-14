package flow

import (
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/pkg/types"
	"github.com/ashwinyue/dcp/pkg/streams"
	"github.com/ashwinyue/dcp/pkg/streams/flow"
)

// TransformFunction represents a transformation function for batch items.
type TransformFunction[T, R any] func(types.ProcessingContext, *types.BatchItem[T]) (*types.BatchItem[R], error)

// TransformFlow represents a flow that transforms batch items from type T to type R.
// It wraps pkg/streams Map and provides transformation functionality.
type TransformFlow[T, R any] struct {
	*flow.Map[*types.BatchItem[T], *types.BatchItem[R]]
	transformFunction TransformFunction[T, R]
	logger            types.Logger
}

// Verify TransformFlow satisfies the Flow interface.
var _ streams.Flow = (*TransformFlow[any, any])(nil)

// NewTransformFlow creates a new transform flow.
//
// transformFunction is the transformation function to apply to each batch item.
// logger is used for logging transformation errors.
// parallelism is the flow parallelism factor. In case the events order matters, use parallelism = 1.
func NewTransformFlow[T, R any](transformFunction TransformFunction[T, R], logger types.Logger, parallelism uint) *TransformFlow[T, R] {
	// Create a map function that transforms items
	mapFunction := func(item *types.BatchItem[T]) *types.BatchItem[R] {
		// Create processing context
		procCtx := types.ProcessingContext{
			TaskID:    item.ID,
			BatchID:   item.ID,
			StartTime: time.Now(),
		}

		transformed, err := transformFunction(procCtx, item)
		if err != nil {
			logger.Error("Transform failed", "itemID", item.ID, "error", err)
			// Return nil or handle error appropriately
			return nil
		}
		return transformed
	}

	tf := &TransformFlow[T, R]{
		Map:               flow.NewMap(mapFunction, parallelism),
		transformFunction: transformFunction,
		logger:            logger,
	}
	return tf
}

// Via, To, In, and Out methods are inherited from embedded Map
