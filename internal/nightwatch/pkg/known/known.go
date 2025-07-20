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
