package known

// MessageBatch job condition types
const (
	// Preparation step conditions
	MessageBatchPreparationReady     = "PreparationReady"
	MessageBatchPreparationRunning   = "PreparationRunning"
	MessageBatchPreparationCompleted = "PreparationCompleted"
	MessageBatchPreparationFailed    = "PreparationFailed"
	MessageBatchPreparationPaused    = "PreparationPaused"

	// Delivery step conditions
	MessageBatchDeliveryReady     = "DeliveryReady"
	MessageBatchDeliveryRunning   = "DeliveryRunning"
	MessageBatchDeliveryCompleted = "DeliveryCompleted"
	MessageBatchDeliveryFailed    = "DeliveryFailed"
	MessageBatchDeliveryPaused    = "DeliveryPaused"

	// Final conditions
	MessageBatchSucceeded = "Succeeded"
	MessageBatchFailed    = "Failed"
	MessageBatchCancelled = "Cancelled"
)

// SmsBatch job condition types
const (
	// Preparation step conditions
	SmsBatchPreparationReady     = "PreparationReady"
	SmsBatchPreparationRunning   = "PreparationRunning"
	SmsBatchPreparationCompleted = "PreparationCompleted"
	SmsBatchPreparationFailed    = "PreparationFailed"
	SmsBatchPreparationPaused    = "PreparationPaused"

	// Delivery step conditions
	SmsBatchDeliveryReady     = "DeliveryReady"
	SmsBatchDeliveryRunning   = "DeliveryRunning"
	SmsBatchDeliveryCompleted = "DeliveryCompleted"
	SmsBatchDeliveryFailed    = "DeliveryFailed"
	SmsBatchDeliveryPaused    = "DeliveryPaused"

	// Final conditions
	SmsBatchSucceeded = "Succeeded"
	SmsBatchFailed    = "Failed"
	SmsBatchCancelled = "Cancelled"
)
