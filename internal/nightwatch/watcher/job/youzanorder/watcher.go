package youzanorder

import (
	"context"
	"fmt"
	"time"

	"github.com/gammazero/workerpool"
	"github.com/onexstack/onexstack/pkg/store/where"
	"github.com/onexstack/onexstack/pkg/watch/registry"
	"go.uber.org/ratelimit"

	"github.com/ashwinyue/dcp/internal/nightwatch/model"
	"github.com/ashwinyue/dcp/internal/nightwatch/store"
	"github.com/ashwinyue/dcp/internal/nightwatch/watcher"
	known "github.com/ashwinyue/dcp/internal/pkg/known/nightwatch"
	"github.com/ashwinyue/dcp/internal/pkg
	"github.com/ashwinyue/dcp/pkg/streams"
	"github.com/ashwinyue/dcp/pkg/streams/flow"
)

// Ensure Watcher implements the registry.Watcher interface.
var _ registry.Watcher = (*Watcher)(nil)

// Limiter holds rate limiters for different operations.
type Limiter struct {
	Fetch    ratelimit.Limiter
	Validate ratelimit.Limiter
	Enrich   ratelimit.Limiter
	Process  ratelimit.Limiter
}

// JobSource implements streams.Source for reading jobs from database.
type JobSource struct {
	store      store.IStore
	scope      string
	watcher    string
	statuses   []string
	interval   time.Duration
	out        chan any
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// NewJobSource creates a new JobSource instance.
func NewJobSource(store store.IStore, scope, watcher string, statuses []string, interval time.Duration) *JobSource {
	ctx, cancel := context.WithCancel(context.Background())

	source := &JobSource{
		store:      store,
		scope:      scope,
		watcher:    watcher,
		statuses:   statuses,
		interval:   interval,
		out:        make(chan any),
		ctx:        ctx,
		cancelFunc: cancel,
	}

	go source.start()
	return source
}

// Out returns the output channel for reading jobs.
func (s *JobSource) Out() <-chan any {
	return s.out
}

// Via connects this source to a flow.
func (s *JobSource) Via(flow streams.Flow) streams.Flow {
	go func() {
		for job := range s.Out() {
			flow.In() <- job
		}
		close(flow.In())
	}()
	return flow
}

// Stop stops the source.
func (s *JobSource) Stop() {
	s.cancelFunc()
}

// start begins the job reading process.
func (s *JobSource) start() {
	defer close(s.out)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	log.Infow("JobSource started", "scope", s.scope, "watcher", s.watcher, "interval", s.interval)

	for {
		select {
		case <-s.ctx.Done():
			log.Infow("JobSource stopped")
			return
		case <-ticker.C:
			s.readJobs()
		}
	}
}

// readJobs reads jobs from database and sends them to the output channel.
func (s *JobSource) readJobs() {
	ctx := context.Background()

	// Build query conditions
	conditions := where.F(
		"scope", s.scope,
		"watcher", s.watcher,
		"status", s.statuses,
		"suspend", known.JobNonSuspended,
	)

	// Query jobs
	_, jobs, err := s.store.Job().List(ctx, conditions)
	if err != nil {
		log.Errorw("Failed to read jobs from database", "error", err)
		return
	}

	if len(jobs) == 0 {
		return
	}

	log.Infow("Read jobs from database", "count", len(jobs), "scope", s.scope, "watcher", s.watcher)

	// Send jobs to output channel
	for _, job := range jobs {
		select {
		case s.out <- job:
		case <-s.ctx.Done():
			return
		}
	}
}

// DatabaseSink implements streams.Sink for writing jobs to database.
type DatabaseSink struct {
	store store.IStore
	in    chan any
}

// NewDatabaseSink creates a new DatabaseSink instance.
func NewDatabaseSink(store store.IStore) *DatabaseSink {
	sink := &DatabaseSink{
		store: store,
		in:    make(chan any),
	}

	go sink.start()
	return sink
}

// In returns the input channel for receiving jobs.
func (s *DatabaseSink) In() chan<- any {
	return s.in
}

// start begins the job writing process.
func (s *DatabaseSink) start() {
	for jobAny := range s.in {
		job, ok := jobAny.(*model.JobM)
		if !ok {
			log.Errorw("Invalid job type received in DatabaseSink")
			continue
		}

		ctx := context.Background()
		err := s.store.Job().Update(ctx, job)
		if err != nil {
			log.Errorw("Failed to update job in database", "jobID", job.JobID, "error", err)
		} else {
			log.Infow("Job updated successfully", "jobID", job.JobID, "status", job.Status)
		}
	}
}

// Watcher monitors and processes YouZan order jobs using streams.
type Watcher struct {
	Store store.IStore

	// Maximum number of concurrent workers.
	MaxWorkers int64
	// Rate limiters for operations.
	Limiter Limiter

	// Streams components
	source *JobSource
	sink   *DatabaseSink

	// YouZan API client
	youZanClient *YouZanClient
}

// Run executes the watcher logic using streams pipeline.
func (w *Watcher) Run() {
	ctx := context.Background()
	log.Infow("Starting YouZan order watcher with streams")

	// Create source for reading jobs
	w.source = NewJobSource(
		w.Store,
		known.YouZanOrderJobScope,
		known.YouZanOrderWatcher,
		[]string{
			known.YouZanOrderPending,
			known.YouZanOrderFetching,
			known.YouZanOrderFetched,
			known.YouZanOrderValidating,
			known.YouZanOrderValidated,
			known.YouZanOrderEnriching,
			known.YouZanOrderEnriched,
			known.YouZanOrderProcessing,
		},
		5*time.Second,
	)

	// Create sink for writing jobs
	w.sink = NewDatabaseSink(w.Store)

	// Create processing pipeline with Map operations
	processor := flow.NewMap(func(jobAny any) any {
		job := jobAny.(*model.JobM)
		return w.processJobWithStreams(ctx, job)
	}, uint(w.MaxWorkers))

	// Connect the pipeline: source -> processor -> sink
	w.source.Via(processor).To(w.sink)

	log.Infow("YouZan order watcher streams pipeline started")
}

// processJobWithStreams processes a single job using streams approach.
func (w *Watcher) processJobWithStreams(ctx context.Context, job *model.JobM) *model.JobM {
	log.Infow("Processing YouZan order job", "jobID", job.JobID, "status", job.Status)

	// Create state machine for this job
	sm := NewStateMachine(job.Status, w, job)

	// Process the job based on current status using state machine
	switch job.Status {
	case known.YouZanOrderPending:
		sm.Fetch()
	case known.YouZanOrderFetching:
		sm.Fetch()
	case known.YouZanOrderFetched:
		sm.Validate()
	case known.YouZanOrderValidating:
		sm.Validate()
	case known.YouZanOrderValidated:
		sm.Enrich()
	case known.YouZanOrderEnriching:
		sm.Enrich()
	case known.YouZanOrderEnriched:
		sm.Process()
	case known.YouZanOrderProcessing:
		sm.Process()
	default:
		log.Infow("Job status does not require processing", "jobID", job.JobID, "status", job.Status)
	}

	return job
}

// Spec returns the cron job specification for scheduling.
func (w *Watcher) Spec() string {
	return known.YouZanOrderWatcherSpec
}

// SetAggregateConfig sets the aggregate configuration for the watcher.
func (w *Watcher) SetAggregateConfig(config *watcher.AggregateConfig) {
	w.Store = config.Store
	w.Limiter = Limiter{
		Fetch:    ratelimit.New(known.YouZanOrderFetchQPS),
		Validate: ratelimit.New(known.YouZanOrderValidateQPS),
		Enrich:   ratelimit.New(known.YouZanOrderEnrichQPS),
		Process:  ratelimit.New(known.YouZanOrderProcessQPS),
	}

	// Initialize YouZan client with configuration
	config := GetDefaultConfig()
	w.youZanClient = NewYouZanClient(
		config.API.BaseURL,
		config.API.AppID,
		config.API.AppSecret,
	)
}

// SetMaxWorkers sets the maximum number of concurrent workers for the watcher.
func (w *Watcher) SetMaxWorkers(maxWorkers int64) {
	w.MaxWorkers = maxWorkers
}

// SetStore sets the store for the watcher (for interface compatibility).
func (w *Watcher) SetStore(store store.IStore) {
	w.Store = store
}

// GetMaxWorkers returns the maximum number of workers.
func (w *Watcher) GetMaxWorkers() int64 {
	return w.MaxWorkers
}

// Stop stops the watcher and all its components.
func (w *Watcher) Stop() {
	if w.source != nil {
		w.source.Stop()
	}
}

// Rate limiting methods for different operations
func (w *Watcher) takeFetchLimit() {
	w.Limiter.Fetch.Take()
}

func (w *Watcher) takeValidateLimit() {
	w.Limiter.Validate.Take()
}

func (w *Watcher) takeEnrichLimit() {
	w.Limiter.Enrich.Take()
}

func (w *Watcher) takeProcessLimit() {
	w.Limiter.Process.Take()
}

// Business logic methods (实现具体的有赞订单业务逻辑)
func (w *Watcher) FetchOrder(ctx context.Context, job *model.JobM) error {
	w.takeFetchLimit()
	log.Infow("Fetching YouZan orders", "jobID", job.JobID)

	// Validate job parameters
	if err := validateJobParameters(job); err != nil {
		return fmt.Errorf("invalid job parameters: %w", err)
	}

	// Build fetch request from job metadata
	req := buildOrderRequest(job)

	// Fetch orders from YouZan API
	response, err := w.youZanClient.FetchOrders(ctx, req)
	if err != nil {
		log.Errorw("Failed to fetch orders from YouZan API", "jobID", job.JobID, "error", err)
		return err
	}

	if !response.Success {
		return fmt.Errorf("YouZan API returned error: %s", response.Message)
	}

	// Store fetched orders in job metadata
	if job.Metadata == nil {
		job.Metadata = make(map[string]interface{})
	}
	job.Metadata["fetched_orders"] = response.Data.Orders
	job.Metadata["total_orders"] = response.Data.TotalCount
	job.Metadata["fetch_time"] = time.Now().Format(time.RFC3339)

	// Update job progress
	if err := updateJobProgress(ctx, w.Store, job); err != nil {
		log.Warnw("Failed to update job progress", "jobID", job.JobID, "error", err)
	}

	log.Infow("Successfully fetched YouZan orders", "jobID", job.JobID, "count", len(response.Data.Orders))
	return nil
}

func (w *Watcher) ValidateOrder(ctx context.Context, job *model.JobM) error {
	w.takeValidateLimit()
	log.Infow("Validating YouZan orders", "jobID", job.JobID)

	// Get orders from job metadata
	ordersData, ok := job.Metadata["fetched_orders"]
	if !ok {
		return fmt.Errorf("no orders found in job metadata")
	}

	// Convert to YouZanOrder slice
	orders, ok := ordersData.([]*YouZanOrder)
	if !ok {
		return fmt.Errorf("invalid orders data format")
	}

	validOrders := make([]*YouZanOrder, 0)
	invalidOrders := make([]*YouZanOrder, 0)

	// Validate each order
	for _, order := range orders {
		if err := w.youZanClient.ValidateOrder(ctx, order); err != nil {
			log.Warnw("Order validation failed", "jobID", job.JobID, "orderID", order.OrderID, "error", err)
			invalidOrders = append(invalidOrders, order)
		} else {
			validOrders = append(validOrders, order)
		}
	}

	// Update job metadata with validation results
	job.Metadata["valid_orders"] = validOrders
	job.Metadata["invalid_orders"] = invalidOrders
	job.Metadata["valid_count"] = len(validOrders)
	job.Metadata["invalid_count"] = len(invalidOrders)
	job.Metadata["validate_time"] = time.Now().Format(time.RFC3339)

	// Update job progress
	if err := updateJobProgress(ctx, w.Store, job); err != nil {
		log.Warnw("Failed to update job progress", "jobID", job.JobID, "error", err)
	}

	log.Infow("Order validation completed", "jobID", job.JobID, "valid", len(validOrders), "invalid", len(invalidOrders))
	return nil
}

func (w *Watcher) EnrichOrder(ctx context.Context, job *model.JobM) error {
	w.takeEnrichLimit()
	log.Infow("Enriching YouZan orders", "jobID", job.JobID)

	// Get valid orders from job metadata
	ordersData, ok := job.Metadata["valid_orders"]
	if !ok {
		return fmt.Errorf("no valid orders found in job metadata")
	}

	orders, ok := ordersData.([]*YouZanOrder)
	if !ok {
		return fmt.Errorf("invalid orders data format")
	}

	enrichedOrders := make([]*YouZanOrder, 0)

	// Enrich each order
	for _, order := range orders {
		if err := w.youZanClient.EnrichOrder(ctx, order); err != nil {
			log.Warnw("Order enrichment failed", "jobID", job.JobID, "orderID", order.OrderID, "error", err)
			continue
		}
		enrichedOrders = append(enrichedOrders, order)
	}

	// Update job metadata with enriched orders
	job.Metadata["enriched_orders"] = enrichedOrders
	job.Metadata["enriched_count"] = len(enrichedOrders)
	job.Metadata["enrich_time"] = time.Now().Format(time.RFC3339)

	// Update job progress
	if err := updateJobProgress(ctx, w.Store, job); err != nil {
		log.Warnw("Failed to update job progress", "jobID", job.JobID, "error", err)
	}

	log.Infow("Order enrichment completed", "jobID", job.JobID, "enriched", len(enrichedOrders))
	return nil
}

func (w *Watcher) ProcessOrder(ctx context.Context, job *model.JobM) error {
	w.takeProcessLimit()
	log.Infow("Processing YouZan orders", "jobID", job.JobID)

	// Get enriched orders from job metadata
	ordersData, ok := job.Metadata["enriched_orders"]
	if !ok {
		return fmt.Errorf("no enriched orders found in job metadata")
	}

	orders, ok := ordersData.([]*YouZanOrder)
	if !ok {
		return fmt.Errorf("invalid orders data format")
	}

	processedOrders := make([]*YouZanOrder, 0)

	// Process each order
	for _, order := range orders {
		// Convert to batch format
		orderBatch := w.youZanClient.ConvertToOrderBatch(job, order)

		// Process the order
		if err := w.youZanClient.ProcessOrder(ctx, order); err != nil {
			log.Warnw("Order processing failed", "jobID", job.JobID, "orderID", order.OrderID, "error", err)
			orderBatch.RecordError(err)
			continue
		}

		processedOrders = append(processedOrders, order)
		orderBatch.MarkCompleted()

		// TODO: Save batch data to persistent storage if needed
		log.Infow("Order batch processed", "jobID", job.JobID, "orderID", order.OrderID, "duration", orderBatch.GetDuration())
	}

	// Update job metadata with processing results
	job.Metadata["processed_orders"] = processedOrders
	job.Metadata["processed_count"] = len(processedOrders)
	job.Metadata["process_time"] = time.Now().Format(time.RFC3339)

	// Update job progress
	if err := updateJobProgress(ctx, w.Store, job); err != nil {
		log.Warnw("Failed to update job progress", "jobID", job.JobID, "error", err)
	}

	log.Infow("Order processing completed", "jobID", job.JobID, "processed", len(processedOrders))
	return nil
}

func init() {
	registry.Register(known.YouZanOrderWatcher, &Watcher{
		MaxWorkers: known.YouZanOrderMaxWorkers,
	})
}
