package batch

import (
	"time"
)

// YouZanOrderBatch represents a batch of YouZan order processing data.
type YouZanOrderBatch struct {
	// Job information
	JobID     string    `json:"job_id"`
	Scope     string    `json:"scope"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`

	// Metadata and configuration
	Metadata map[string]interface{} `json:"metadata,omitempty"`

	// Order processing data
	OrderData *YouZanOrderData `json:"order_data,omitempty"`

	// Processing metrics
	Metrics *YouZanProcessingMetrics `json:"metrics,omitempty"`
}

// YouZanOrderData contains the actual order data from YouZan.
type YouZanOrderData struct {
	OrderID      string                 `json:"order_id"`
	OrderNumber  string                 `json:"order_number"`
	OrderStatus  string                 `json:"order_status"`
	RefundStatus string                 `json:"refund_status"`
	TotalAmount  float64                `json:"total_amount"`
	Currency     string                 `json:"currency"`
	CustomerInfo *CustomerInfo          `json:"customer_info,omitempty"`
	OrderItems   []*OrderItem           `json:"order_items,omitempty"`
	OrderTime    time.Time              `json:"order_time"`
	UpdateTime   time.Time              `json:"update_time"`
	RawData      map[string]interface{} `json:"raw_data,omitempty"`
}

// CustomerInfo contains customer information.
type CustomerInfo struct {
	CustomerID   string `json:"customer_id"`
	CustomerName string `json:"customer_name"`
	Phone        string `json:"phone"`
	Email        string `json:"email"`
	Address      string `json:"address"`
}

// OrderItem represents an item in the order.
type OrderItem struct {
	ItemID      string  `json:"item_id"`
	ProductID   string  `json:"product_id"`
	ProductName string  `json:"product_name"`
	SKU         string  `json:"sku"`
	Quantity    int     `json:"quantity"`
	UnitPrice   float64 `json:"unit_price"`
	TotalPrice  float64 `json:"total_price"`
	BrandID     string  `json:"brand_id"`
	CategoryID  string  `json:"category_id"`
}

// YouZanProcessingMetrics contains YouZan-specific processing metrics.
type YouZanProcessingMetrics struct {
	StartTime    time.Time     `json:"start_time"`
	EndTime      *time.Time    `json:"end_time,omitempty"`
	Duration     time.Duration `json:"duration"`
	RetryCount   int           `json:"retry_count"`
	ErrorCount   int           `json:"error_count"`
	LastError    string        `json:"last_error,omitempty"`
	ProcessedBy  string        `json:"processed_by"`
	ProcessingID string        `json:"processing_id"`
}

// IsCompleted returns true if the batch processing is completed.
func (b *YouZanOrderBatch) IsCompleted() bool {
	return b.Metrics != nil && b.Metrics.EndTime != nil
}

// GetDuration returns the processing duration.
func (b *YouZanOrderBatch) GetDuration() time.Duration {
	if b.Metrics == nil {
		return 0
	}
	if b.Metrics.EndTime == nil {
		return time.Since(b.Metrics.StartTime)
	}
	return b.Metrics.EndTime.Sub(b.Metrics.StartTime)
}

// MarkCompleted marks the batch as completed.
func (b *YouZanOrderBatch) MarkCompleted() {
	if b.Metrics == nil {
		b.Metrics = &YouZanProcessingMetrics{
			StartTime: time.Now(),
		}
	}
	now := time.Now()
	b.Metrics.EndTime = &now
	b.Metrics.Duration = b.GetDuration()
}

// IncrementRetry increments the retry count.
func (b *YouZanOrderBatch) IncrementRetry() {
	if b.Metrics == nil {
		b.Metrics = &YouZanProcessingMetrics{
			StartTime: time.Now(),
		}
	}
	b.Metrics.RetryCount++
}

// RecordError records an error in the batch.
func (b *YouZanOrderBatch) RecordError(err error) {
	if b.Metrics == nil {
		b.Metrics = &YouZanProcessingMetrics{
			StartTime: time.Now(),
		}
	}
	b.Metrics.ErrorCount++
	b.Metrics.LastError = err.Error()
}
