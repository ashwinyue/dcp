package flow

import (
	"time"

	"github.com/ashwinyue/dcp/internal/nightwatch/pkg/types"
	"github.com/ashwinyue/dcp/pkg/streams"
	"github.com/ashwinyue/dcp/pkg/streams/flow"
)

// ValidationFunction represents a validation function for batch items.
type ValidationFunction func(types.ProcessingContext, *types.BatchItem[*types.BatchTask]) error

// ValidationFlow represents a flow that validates batch items.
// It wraps pkg/streams Filter and provides validation functionality.
type ValidationFlow struct {
	*flow.Filter[*types.BatchTask]
	validationFunction ValidationFunction
	logger             types.Logger
}

// Verify ValidationFlow satisfies the Flow interface.
var _ streams.Flow = (*ValidationFlow)(nil)

// NewValidationFlow creates a new validation flow.
//
// validationFunction is the validation function to apply to each batch item.
// logger is used for logging validation errors.
// parallelism is the flow parallelism factor. In case the events order matters, use parallelism = 1.
func NewValidationFlow(validationFunction ValidationFunction, logger types.Logger, parallelism uint) *ValidationFlow {
	// Create a filter predicate that validates tasks
	validationPredicate := func(task *types.BatchTask) bool {
		item := &types.BatchItem[*types.BatchTask]{
			ID:        task.ID,
			Data:      task,
			CreatedAt: task.CreatedAt,
		}

		// Create processing context
		procCtx := types.ProcessingContext{
			TaskID:    task.ID,
			BatchID:   task.ID,
			StartTime: time.Now(),
		}

		if err := validationFunction(procCtx, item); err != nil {
			task.Status = types.TaskStatusFailed
			task.Error = &types.TaskError{
				Code:      "VALIDATION_ERROR",
				Message:   err.Error(),
				Timestamp: time.Now(),
			}
			logger.Error("Task validation failed", "taskID", task.ID, "error", err)
			return false // Filter out invalid tasks
		}
		return true // Pass valid tasks
	}

	vf := &ValidationFlow{
		Filter:             flow.NewFilter(validationPredicate, parallelism),
		validationFunction: validationFunction,
		logger:             logger,
	}
	return vf
}

// Via, To, In, and Out methods are inherited from embedded Filter
