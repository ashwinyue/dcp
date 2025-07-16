package known

import (
	"time"

	stringsutil "github.com/onexstack/onexstack/pkg/util/strings"
)

// Message Batch processing statuses represent the various phases of message batch processing.
const (
	// MessageBatchSucceeded indicates that the message batch has successfully completed.
	MessageBatchSucceeded = JobSucceeded
	// MessageBatchFailed indicates that the message batch has failed.
	MessageBatchFailed = JobFailed

	// MessageBatchPending indicates that the message batch is pending.
	MessageBatchPending = JobPending
	// MessageBatchProcessing indicates that the message batch is currently being processed.
	MessageBatchProcessing = "Processing"
	// MessageBatchCompleted indicates that the message batch has completed successfully.
	MessageBatchCompleted = "Completed"
	// MessageBatchPartialComplete indicates that the message batch has partially completed.
	MessageBatchPartialComplete = "PartialComplete"
	// MessageBatchRetrying indicates that the message batch is retrying failed operations.
	MessageBatchRetrying = "Retrying"

	// MessageBatchPreparationReady indicates that the batch is ready for preparation.
	MessageBatchPreparationReady = "PreparationReady"
	// MessageBatchPreparationRunning indicates that the preparation phase is running.
	MessageBatchPreparationRunning = "PreparationRunning"
	// MessageBatchPreparationPausing indicates that the preparation phase is pausing.
	MessageBatchPreparationPausing = "PreparationPausing"
	// MessageBatchPreparationPaused indicates that the preparation phase is paused.
	MessageBatchPreparationPaused = "PreparationPaused"
	// MessageBatchPreparationCompleted indicates that the preparation phase has completed.
	MessageBatchPreparationCompleted = "PreparationCompleted"
	// MessageBatchPreparationFailed indicates that the preparation phase has failed.
	MessageBatchPreparationFailed = "PreparationFailed"

	// MessageBatchDeliveryPending indicates that the delivery phase is pending.
	MessageBatchDeliveryPending = "DeliveryPending"
	// MessageBatchDeliveryReady indicates that the batch is ready for delivery.
	MessageBatchDeliveryReady = "DeliveryReady"
	// MessageBatchDeliveryRunning indicates that the delivery phase is running.
	MessageBatchDeliveryRunning = "DeliveryRunning"
	// MessageBatchDeliveryPausing indicates that the delivery phase is pausing.
	MessageBatchDeliveryPausing = "DeliveryPausing"
	// MessageBatchDeliveryPaused indicates that the delivery phase is paused.
	MessageBatchDeliveryPaused = "DeliveryPaused"
	// MessageBatchDeliveryCompleted indicates that the delivery phase has completed.
	MessageBatchDeliveryCompleted = "DeliveryCompleted"
	// MessageBatchDeliveryFailed indicates that the delivery phase has failed.
	MessageBatchDeliveryFailed = "DeliveryFailed"
)

// Message Batch FSM events for PREPARATION phase
const (
	// MessageBatchEventPrepareStart starts the preparation phase
	MessageBatchEventPrepareStart = "prepare_start"
	// MessageBatchEventPrepareBegin begins the preparation execution
	MessageBatchEventPrepareBegin = "prepare_begin"
	// MessageBatchEventPreparePause pauses the preparation
	MessageBatchEventPreparePause = "prepare_pause"
	// MessageBatchEventPreparePaused confirms the preparation is paused
	MessageBatchEventPreparePaused = "prepare_paused"
	// MessageBatchEventPrepareResume resumes the preparation
	MessageBatchEventPrepareResume = "prepare_resume"
	// MessageBatchEventPrepareComplete completes the preparation successfully
	MessageBatchEventPrepareComplete = "prepare_complete"
	// MessageBatchEventPrepareFail fails the preparation
	MessageBatchEventPrepareFail = "prepare_fail"
	// MessageBatchEventPrepareRetry retries the preparation after failure
	MessageBatchEventPrepareRetry = "prepare_retry"
)

// Message Batch FSM events for DELIVERY phase
const (
	// MessageBatchEventDeliveryStart starts the delivery phase
	MessageBatchEventDeliveryStart = "delivery_start"
	// MessageBatchEventDeliveryBegin begins the delivery execution
	MessageBatchEventDeliveryBegin = "delivery_begin"
	// MessageBatchEventDeliveryPause pauses the delivery
	MessageBatchEventDeliveryPause = "delivery_pause"
	// MessageBatchEventDeliveryPaused confirms the delivery is paused
	MessageBatchEventDeliveryPaused = "delivery_paused"
	// MessageBatchEventDeliveryResume resumes the delivery
	MessageBatchEventDeliveryResume = "delivery_resume"
	// MessageBatchEventDeliveryComplete completes the delivery successfully
	MessageBatchEventDeliveryComplete = "delivery_complete"
	// MessageBatchEventDeliveryFail fails the delivery
	MessageBatchEventDeliveryFail = "delivery_fail"
	// MessageBatchEventDeliveryRetry retries the delivery after failure
	MessageBatchEventDeliveryRetry = "delivery_retry"
)

// Message Batch step types
const (
	// MessageBatchStepPreparation represents the preparation step
	MessageBatchStepPreparation = "preparation"
	// MessageBatchStepDelivery represents the delivery step
	MessageBatchStepDelivery = "delivery"
)

// Message Batch configuration constants
const (
	// MessageBatchTimeout defines the maximum duration (in seconds) for message batch processing.
	MessageBatchTimeout = 7200 // 2 hours

	// MessageBatchMaxWorkers specify the maximum number of workers for message batch processing.
	MessageBatchMaxWorkers = 10

	// MessageBatchPreparationMaxWorkers specify the maximum number of workers for preparation phase.
	MessageBatchPreparationMaxWorkers = 5

	// MessageBatchDeliveryMaxWorkers specify the maximum number of workers for delivery phase.
	MessageBatchDeliveryMaxWorkers = 8

	// MessageBatchDefaultBatchSize specify the default batch size for processing.
	MessageBatchDefaultBatchSize = 1000

	// MessageBatchPartitionNum specify the number of partitions for distributed processing.
	MessageBatchPartitionNum = 128

	// MessageBatchPartitionCount specify the number of partitions (alias for MessageBatchPartitionNum).
	MessageBatchPartitionCount = MessageBatchPartitionNum

	// MessageBatchVirtualNodes specify the number of virtual nodes for consistent hashing.
	MessageBatchVirtualNodes = 256

	// MessageBatchDefaultPartitionKey specify the default partition key.
	MessageBatchDefaultPartitionKey = "default"

	// MessageBatchPreparationTimeout defines the timeout for preparation phase (in seconds).
	MessageBatchPreparationTimeout = 300 // 5 minutes

	// MessageBatchDeliveryTimeout defines the timeout for delivery phase (in seconds).
	MessageBatchDeliveryTimeout = 600 // 10 minutes

	// MessageBatchMaxRetries defines the maximum number of retries for each phase.
	MessageBatchMaxRetries = 3

	// MessageBatchProcessingQPS specify the maximum queries per second for processing.
	MessageBatchProcessingQPS = 50

	// MessageBatchSendQPS specify the maximum queries per second for sending.
	MessageBatchSendQPS = 100

	// MessageBatchMessagingQPS specify the maximum queries per second for messaging.
	MessageBatchMessagingQPS = 80
)

// Message Batch job scope and watcher identifiers
const (
	// MessageBatchJobScope defines the scope for message batch jobs.
	MessageBatchJobScope = "messagebatch"
	// MessageBatchWatcher defines the watcher name for message batch jobs.
	MessageBatchWatcher = "messagebatch"
)

// Message Batch processing timeout durations
var (
	// MessageBatchPreparationProcessTimeout defines the timeout for preparation processing
	MessageBatchPreparationProcessTimeout = 5 * time.Minute
	// MessageBatchDeliveryProcessTimeout defines the timeout for delivery processing
	MessageBatchDeliveryProcessTimeout = 10 * time.Minute
)

// Message Batch phases in order
var MessageBatchPhases = []string{
	MessageBatchStepPreparation,
	MessageBatchStepDelivery,
}

// StandardMessageBatchStatus normalizes message batch status for display purposes.
func StandardMessageBatchStatus(status string) string {
	if !stringsutil.StringIn(status, []string{MessageBatchFailed, MessageBatchSucceeded, MessageBatchPending}) {
		return JobRunning
	}
	return status
}

// GetNextMessageBatchPhase returns the next phase in the message batch processing pipeline.
func GetNextMessageBatchPhase(currentPhase string) string {
	for i, phase := range MessageBatchPhases {
		if phase == currentPhase && i < len(MessageBatchPhases)-1 {
			return MessageBatchPhases[i+1]
		}
	}
	return MessageBatchSucceeded
}

// IsValidMessageBatchPhase checks if the given phase is a valid message batch processing phase.
func IsValidMessageBatchPhase(phase string) bool {
	return stringsutil.StringIn(phase, MessageBatchPhases)
}

// MessageBatch helper functions

// IsPreparationState checks if the given state belongs to the preparation phase
func IsPreparationState(state string) bool {
	preparationStates := []string{
		MessageBatchPreparationReady,
		MessageBatchPreparationRunning,
		MessageBatchPreparationPausing,
		MessageBatchPreparationPaused,
		MessageBatchPreparationCompleted,
		MessageBatchPreparationFailed,
	}

	return stringsutil.StringIn(state, preparationStates)
}

// IsDeliveryState checks if the given state belongs to the delivery phase
func IsDeliveryState(state string) bool {
	deliveryStates := []string{
		MessageBatchDeliveryPending,
		MessageBatchDeliveryReady,
		MessageBatchDeliveryRunning,
		MessageBatchDeliveryPausing,
		MessageBatchDeliveryPaused,
		MessageBatchDeliveryCompleted,
		MessageBatchDeliveryFailed,
	}

	return stringsutil.StringIn(state, deliveryStates)
}

// IsTerminalState checks if the given state is terminal (no further transitions)
func IsTerminalState(state string) bool {
	terminalStates := []string{
		MessageBatchPreparationCompleted,
		MessageBatchDeliveryCompleted,
		MessageBatchSucceeded,
	}

	return stringsutil.StringIn(state, terminalStates)
}

// IsFailedState checks if the given state represents a failure
func IsFailedState(state string) bool {
	failedStates := []string{
		MessageBatchPreparationFailed,
		MessageBatchDeliveryFailed,
		MessageBatchFailed,
	}

	return stringsutil.StringIn(state, failedStates)
}

// GetPhaseFromState returns the phase name for a given state
func GetPhaseFromState(state string) string {
	if IsPreparationState(state) {
		return "PREPARATION"
	} else if IsDeliveryState(state) {
		return "DELIVERY"
	}
	return "UNKNOWN"
}
