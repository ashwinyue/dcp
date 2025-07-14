package flow

import (
	"github.com/ashwinyue/dcp/internal/nightwatch/pkg/types"
	"github.com/ashwinyue/dcp/pkg/streams"
	"github.com/ashwinyue/dcp/pkg/streams/flow"
)

// FilterPredicate represents a filter predicate function for batch processing.
type FilterPredicate func(any) bool

// FilterFlow represents a flow that filters batch items based on a predicate.
// It wraps pkg/streams Filter and provides filtering functionality.
type FilterFlow struct {
	*flow.Filter[any]
	filterPredicate FilterPredicate
	logger          types.Logger
}

// Verify FilterFlow satisfies the Flow interface.
var _ streams.Flow = (*FilterFlow)(nil)

// NewFilterFlow creates a new filter flow.
//
// filterPredicate is the boolean-valued filter function.
// logger is used for logging.
// parallelism is the flow parallelism factor. In case the events order matters, use parallelism = 1.
func NewFilterFlow(filterPredicate FilterPredicate, logger types.Logger, parallelism uint) *FilterFlow {
	// Convert our FilterPredicate to flow.FilterPredicate[any]
	flowPredicate := func(item any) bool {
		return filterPredicate(item)
	}

	ff := &FilterFlow{
		Filter:          flow.NewFilter[any](flowPredicate, parallelism),
		filterPredicate: filterPredicate,
		logger:          logger,
	}
	return ff
}

// Via, To, In, and Out methods are inherited from embedded Filter
