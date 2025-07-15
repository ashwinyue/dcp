package known

import (
	"time"

	stringsutil "github.com/onexstack/onexstack/pkg/util/strings"
)

// Data layer processing statuses represent the various phases of data layer transformation.
const (
	// DataLayerSucceeded indicates that the data layer processing has successfully completed.
	DataLayerSucceeded = JobSucceeded
	// DataLayerFailed indicates that the data layer processing has failed.
	DataLayerFailed = JobFailed

	// DataLayerPending indicates that the data layer processing is pending.
	DataLayerPending = JobPending
	// DataLayerLandingToODS indicates that data is being processed from landing to ODS.
	DataLayerLandingToODS = "LandingToODS"
	// DataLayerODSToDWD indicates that data is being processed from ODS to DWD.
	DataLayerODSToDWD = "ODSToDWD"
	// DataLayerDWDToDWS indicates that data is being processed from DWD to DWS.
	DataLayerDWDToDWS = "DWDToDWS"
	// DataLayerDWSToDS indicates that data is being processed from DWS to DS.
	DataLayerDWSToDS = "DWSToDS"
	// DataLayerCompleted indicates that all data layer processing has completed.
	DataLayerCompleted = "Completed"
)

// Data layer FSM events
const (
	// DataLayerEventStart starts the data layer processing
	DataLayerEventStart = "start"
	// DataLayerEventLandingToODSComplete completes the landing to ODS transformation
	DataLayerEventLandingToODSComplete = "landing_to_ods_complete"
	// DataLayerEventODSToDWDComplete completes the ODS to DWD transformation
	DataLayerEventODSToDWDComplete = "ods_to_dwd_complete"
	// DataLayerEventDWDToDWSComplete completes the DWD to DWS transformation
	DataLayerEventDWDToDWSComplete = "dwd_to_dws_complete"
	// DataLayerEventDWSToDS completes the DWS to DS transformation
	DataLayerEventDWSToDS = "dws_to_ds_complete"
	// DataLayerEventError indicates an error occurred
	DataLayerEventError = "error"
	// DataLayerEventComplete indicates completion of all processing
	DataLayerEventComplete = "complete"
)

// Data layer types
const (
	// DataLayerLanding represents the landing layer (raw data).
	DataLayerLanding = "landing"
	// DataLayerODS represents the Operational Data Store layer.
	DataLayerODS = "ods"
	// DataLayerDWD represents the Data Warehouse Detail layer.
	DataLayerDWD = "dwd"
	// DataLayerDWS represents the Data Warehouse Summary layer.
	DataLayerDWS = "dws"
	// DataLayerDS represents the Data Service layer.
	DataLayerDS = "ds"
)

// Data layer processing configuration constants.
const (
	// DataLayerTimeout defines the maximum duration (in seconds) for data layer processing.
	DataLayerTimeout = 3600 // 1 hour

	// DataLayerMaxWorkers specify the maximum number of workers for data layer processing.
	DataLayerMaxWorkers = 5

	// DataLayerBatchSize specify the default batch size for data layer processing.
	DataLayerBatchSize = 10000

	// DataLayerBufferSize specify the default buffer size for data layer channels.
	DataLayerBufferSize = 1000
)

// Data layer processing timeout duration
var (
	// DataLayerProcessTimeout defines the timeout for each data layer processing step
	DataLayerProcessTimeout = 30 * time.Second
)

// Batch job configuration constants.
const (
	// BatchJobScope defines the scope for batch jobs.
	BatchJobScope = "batch"
	// BatchJobWatcher defines the watcher name for batch jobs.
	BatchJobWatcher = "batch"

	// BatchJobTimeout defines the maximum duration (in seconds) for batch jobs.
	BatchJobTimeout = 7200 // 2 hours

	// BatchJobMaxWorkers specify the maximum number of workers for batch jobs.
	BatchJobMaxWorkers = 10

	// BatchJobDefaultConcurrency specify the default concurrency for batch processing.
	BatchJobDefaultConcurrency = 5
)

// Data layer transformation steps in order.
var DataLayerSteps = []string{
	DataLayerLandingToODS,
	DataLayerODSToDWD,
	DataLayerDWDToDWS,
	DataLayerDWSToDS,
}

// StandardDataLayerStatus normalizes data layer processing status for display purposes.
func StandardDataLayerStatus(status string) string {
	if !stringsutil.StringIn(status, []string{DataLayerFailed, DataLayerSucceeded, DataLayerPending}) {
		return JobRunning
	}
	return status
}

// GetNextDataLayerStep returns the next step in the data layer processing pipeline.
func GetNextDataLayerStep(currentStep string) string {
	for i, step := range DataLayerSteps {
		if step == currentStep && i < len(DataLayerSteps)-1 {
			return DataLayerSteps[i+1]
		}
	}
	return DataLayerCompleted
}

// IsValidDataLayerStep checks if the given step is a valid data layer processing step.
func IsValidDataLayerStep(step string) bool {
	return stringsutil.StringIn(step, DataLayerSteps)
}
