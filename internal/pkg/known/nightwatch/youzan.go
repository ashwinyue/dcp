package known

// YouZan order job status constants
// These constants define the various states that a YouZan order processing job can be in.
const (
	// YouZanOrderSucceeded indicates the YouZan order job has completed successfully.
	YouZanOrderSucceeded = JobSucceeded
	// YouZanOrderFailed indicates the YouZan order job has failed.
	YouZanOrderFailed = JobFailed

	// YouZanOrderPending indicates the YouZan order job is pending execution.
	YouZanOrderPending = JobPending

	// YouZanOrderFetching indicates the job is fetching order data from YouZan API.
	YouZanOrderFetching = "Fetching"

	// YouZanOrderFetched indicates order data has been successfully fetched.
	YouZanOrderFetched = "Fetched"

	// YouZanOrderValidating indicates the job is validating fetched order data.
	YouZanOrderValidating = "Validating"

	// YouZanOrderValidated indicates order data has been validated.
	YouZanOrderValidated = "Validated"

	// YouZanOrderEnriching indicates the job is enriching order data with additional information.
	YouZanOrderEnriching = "Enriching"

	// YouZanOrderEnriched indicates order data has been enriched.
	YouZanOrderEnriched = "Enriched"

	// YouZanOrderProcessing indicates the job is processing the enriched order data.
	YouZanOrderProcessing = "Processing"

	// YouZanOrderProcessed indicates order data has been processed successfully.
	YouZanOrderProcessed = "Processed"
)

// YouZan order job configuration constants
const (
	// YouZanOrderTimeout defines the maximum time (in seconds) for a YouZan order job to complete.
	YouZanOrderTimeout = 7200 // 2 hours

	// YouZanOrderWatcher identifier for YouZan order job watcher.
	YouZanOrderWatcher = "youzanorder"

	// YouZanOrderJobScope defines the scope for YouZan order jobs.
	YouZanOrderJobScope = "youzanorder"

	// YouZanOrderMaxWorkers defines the maximum number of concurrent workers for YouZan order processing.
	YouZanOrderMaxWorkers = 10

	// YouZanOrderAPIQPS defines the QPS limit for YouZan API calls.
	YouZanOrderAPIQPS = 50

	// YouZanOrderBatchSize defines the default batch size for processing orders.
	YouZanOrderBatchSize = 100

	// QPS limits for different operations
	YouZanOrderFetchQPS    = 10
	YouZanOrderValidateQPS = 20
	YouZanOrderEnrichQPS   = 15
	YouZanOrderProcessQPS  = 25
)

// YouZan order processing limits
const (
	// MaxYouZanOrdersPerJob defines the maximum number of orders that can be processed in a single job.
	MaxYouZanOrdersPerJob = 10000

	// YouZanOrderRetryLimit defines the maximum number of retries for failed order processing.
	YouZanOrderRetryLimit = 3
)
