package model

import (
	"database/sql/driver"
	"encoding/json"
	"time"
)

// DataLayerRecord 数据层记录的通用结构
type DataLayerRecord struct {
	ID          int64                  `gorm:"column:id;type:bigint(20) unsigned;primaryKey;autoIncrement:true;comment:主键 ID" json:"id"`
	JobID       string                 `gorm:"column:job_id;type:varchar(100);not null;index:idx_job_id;comment:关联的 Job ID" json:"job_id"`
	RecordID    string                 `gorm:"column:record_id;type:varchar(100);not null;uniqueIndex:idx_record_id;comment:记录唯一标识" json:"record_id"`
	Layer       string                 `gorm:"column:layer;type:varchar(20);not null;index:idx_layer;comment:数据层类型" json:"layer"`
	Status      string                 `gorm:"column:status;type:varchar(20);not null;default:'pending';comment:处理状态" json:"status"`
	Data        map[string]interface{} `gorm:"column:data;type:longtext;comment:数据内容" json:"data"`
	Metadata    map[string]interface{} `gorm:"column:metadata;type:longtext;comment:元数据" json:"metadata"`
	SourceLayer string                 `gorm:"column:source_layer;type:varchar(20);comment:源数据层" json:"source_layer"`
	CreatedAt   time.Time              `gorm:"column:created_at;type:timestamp;not null;default:current_timestamp();comment:创建时间" json:"created_at"`
	UpdatedAt   time.Time              `gorm:"column:updated_at;type:timestamp;not null;default:current_timestamp();comment:更新时间" json:"updated_at"`
}

// TableName 设置表名
func (DataLayerRecord) TableName() string {
	return "nw_data_layer_record"
}

// Scan implements the sql Scanner interface for Data field
func (r *DataLayerRecord) Scan(value interface{}) error {
	if value == nil {
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return nil
	}

	return json.Unmarshal(bytes, r)
}

// Value implements the sql Valuer interface for Data field
func (r *DataLayerRecord) Value() (driver.Value, error) {
	return json.Marshal(r)
}

// LandingLayerRecord Landing层数据记录
type LandingLayerRecord struct {
	ID               int64                  `gorm:"column:id;type:bigint(20) unsigned;primaryKey;autoIncrement:true;comment:主键 ID" json:"id"`
	JobID            string                 `gorm:"column:job_id;type:varchar(100);not null;index:idx_job_id;comment:关联的 Job ID" json:"job_id"`
	RecordID         string                 `gorm:"column:record_id;type:varchar(100);not null;uniqueIndex:idx_record_id;comment:记录唯一标识" json:"record_id"`
	SourceSystem     string                 `gorm:"column:source_system;type:varchar(100);not null;comment:源系统" json:"source_system"`
	RawData          map[string]interface{} `gorm:"column:raw_data;type:longtext;comment:原始数据" json:"raw_data"`
	IngestionTime    time.Time              `gorm:"column:ingestion_time;type:timestamp;not null;comment:摄取时间" json:"ingestion_time"`
	DataFormat       string                 `gorm:"column:data_format;type:varchar(50);comment:数据格式" json:"data_format"`
	DataSize         int64                  `gorm:"column:data_size;type:bigint(20);comment:数据大小(字节)" json:"data_size"`
	ValidationStatus string                 `gorm:"column:validation_status;type:varchar(20);default:'pending';comment:验证状态" json:"validation_status"`
	CreatedAt        time.Time              `gorm:"column:created_at;type:timestamp;not null;default:current_timestamp();comment:创建时间" json:"created_at"`
	UpdatedAt        time.Time              `gorm:"column:updated_at;type:timestamp;not null;default:current_timestamp();comment:更新时间" json:"updated_at"`
}

// TableName 设置表名
func (LandingLayerRecord) TableName() string {
	return "nw_landing_layer"
}

// ODSLayerRecord ODS层数据记录
type ODSLayerRecord struct {
	ID               int64                  `gorm:"column:id;type:bigint(20) unsigned;primaryKey;autoIncrement:true;comment:主键 ID" json:"id"`
	JobID            string                 `gorm:"column:job_id;type:varchar(100);not null;index:idx_job_id;comment:关联的 Job ID" json:"job_id"`
	RecordID         string                 `gorm:"column:record_id;type:varchar(100);not null;uniqueIndex:idx_record_id;comment:记录唯一标识" json:"record_id"`
	SourceRecordID   string                 `gorm:"column:source_record_id;type:varchar(100);not null;index:idx_source_record_id;comment:源记录ID" json:"source_record_id"`
	CleanedData      map[string]interface{} `gorm:"column:cleaned_data;type:longtext;comment:清洗后数据" json:"cleaned_data"`
	ValidationStatus string                 `gorm:"column:validation_status;type:varchar(20);default:'validated';comment:验证状态" json:"validation_status"`
	QualityScore     float64                `gorm:"column:quality_score;type:decimal(5,2);comment:数据质量分数" json:"quality_score"`
	Standardized     bool                   `gorm:"column:standardized;type:tinyint(1);default:1;comment:是否标准化" json:"standardized"`
	DataSchema       string                 `gorm:"column:data_schema;type:text;comment:数据模式" json:"data_schema"`
	CreatedAt        time.Time              `gorm:"column:created_at;type:timestamp;not null;default:current_timestamp();comment:创建时间" json:"created_at"`
	UpdatedAt        time.Time              `gorm:"column:updated_at;type:timestamp;not null;default:current_timestamp();comment:更新时间" json:"updated_at"`
}

// TableName 设置表名
func (ODSLayerRecord) TableName() string {
	return "nw_ods_layer"
}

// DWDLayerRecord DWD层数据记录
type DWDLayerRecord struct {
	ID                   int64                  `gorm:"column:id;type:bigint(20) unsigned;primaryKey;autoIncrement:true;comment:主键 ID" json:"id"`
	JobID                string                 `gorm:"column:job_id;type:varchar(100);not null;index:idx_job_id;comment:关联的 Job ID" json:"job_id"`
	RecordID             string                 `gorm:"column:record_id;type:varchar(100);not null;uniqueIndex:idx_record_id;comment:记录唯一标识" json:"record_id"`
	SourceRecordID       string                 `gorm:"column:source_record_id;type:varchar(100);not null;index:idx_source_record_id;comment:源记录ID" json:"source_record_id"`
	EnrichedData         map[string]interface{} `gorm:"column:enriched_data;type:longtext;comment:增强后数据" json:"enriched_data"`
	BusinessRulesApplied bool                   `gorm:"column:business_rules_applied;type:tinyint(1);default:1;comment:是否应用业务规则" json:"business_rules_applied"`
	DimensionKeys        string                 `gorm:"column:dimension_keys;type:text;comment:维度键" json:"dimension_keys"`
	FactTableName        string                 `gorm:"column:fact_table_name;type:varchar(100);comment:事实表名" json:"fact_table_name"`
	DimensionTableNames  string                 `gorm:"column:dimension_table_names;type:text;comment:维度表名列表" json:"dimension_table_names"`
	DataLineage          string                 `gorm:"column:data_lineage;type:text;comment:数据血缘" json:"data_lineage"`
	CreatedAt            time.Time              `gorm:"column:created_at;type:timestamp;not null;default:current_timestamp();comment:创建时间" json:"created_at"`
	UpdatedAt            time.Time              `gorm:"column:updated_at;type:timestamp;not null;default:current_timestamp();comment:更新时间" json:"updated_at"`
}

// TableName 设置表名
func (DWDLayerRecord) TableName() string {
	return "nw_dwd_layer"
}

// DWSLayerRecord DWS层数据记录
type DWSLayerRecord struct {
	ID               int64                  `gorm:"column:id;type:bigint(20) unsigned;primaryKey;autoIncrement:true;comment:主键 ID" json:"id"`
	JobID            string                 `gorm:"column:job_id;type:varchar(100);not null;index:idx_job_id;comment:关联的 Job ID" json:"job_id"`
	RecordID         string                 `gorm:"column:record_id;type:varchar(100);not null;uniqueIndex:idx_record_id;comment:记录唯一标识" json:"record_id"`
	SourceRecordID   string                 `gorm:"column:source_record_id;type:varchar(100);not null;index:idx_source_record_id;comment:源记录ID" json:"source_record_id"`
	AggregatedData   map[string]interface{} `gorm:"column:aggregated_data;type:longtext;comment:聚合数据" json:"aggregated_data"`
	SummaryType      string                 `gorm:"column:summary_type;type:varchar(50);comment:汇总类型" json:"summary_type"`
	AggregationLevel string                 `gorm:"column:aggregation_level;type:varchar(50);comment:聚合级别" json:"aggregation_level"`
	TimeWindow       string                 `gorm:"column:time_window;type:varchar(50);comment:时间窗口" json:"time_window"`
	MetricNames      string                 `gorm:"column:metric_names;type:text;comment:指标名称列表" json:"metric_names"`
	MetricValues     string                 `gorm:"column:metric_values;type:text;comment:指标值" json:"metric_values"`
	CreatedAt        time.Time              `gorm:"column:created_at;type:timestamp;not null;default:current_timestamp();comment:创建时间" json:"created_at"`
	UpdatedAt        time.Time              `gorm:"column:updated_at;type:timestamp;not null;default:current_timestamp();comment:更新时间" json:"updated_at"`
}

// TableName 设置表名
func (DWSLayerRecord) TableName() string {
	return "nw_dws_layer"
}

// DSLayerRecord DS层数据记录
type DSLayerRecord struct {
	ID             int64                  `gorm:"column:id;type:bigint(20) unsigned;primaryKey;autoIncrement:true;comment:主键 ID" json:"id"`
	JobID          string                 `gorm:"column:job_id;type:varchar(100);not null;index:idx_job_id;comment:关联的 Job ID" json:"job_id"`
	RecordID       string                 `gorm:"column:record_id;type:varchar(100);not null;uniqueIndex:idx_record_id;comment:记录唯一标识" json:"record_id"`
	SourceRecordID string                 `gorm:"column:source_record_id;type:varchar(100);not null;index:idx_source_record_id;comment:源记录ID" json:"source_record_id"`
	ServiceData    map[string]interface{} `gorm:"column:service_data;type:longtext;comment:服务数据" json:"service_data"`
	APIReady       bool                   `gorm:"column:api_ready;type:tinyint(1);default:1;comment:是否API就绪" json:"api_ready"`
	ServiceFormat  string                 `gorm:"column:service_format;type:varchar(50);default:'json';comment:服务格式" json:"service_format"`
	CacheKey       string                 `gorm:"column:cache_key;type:varchar(200);comment:缓存键" json:"cache_key"`
	CacheExpiry    int64                  `gorm:"column:cache_expiry;type:bigint(20);comment:缓存过期时间" json:"cache_expiry"`
	IndexKeys      string                 `gorm:"column:index_keys;type:text;comment:索引键" json:"index_keys"`
	CreatedAt      time.Time              `gorm:"column:created_at;type:timestamp;not null;default:current_timestamp();comment:创建时间" json:"created_at"`
	UpdatedAt      time.Time              `gorm:"column:updated_at;type:timestamp;not null;default:current_timestamp();comment:更新时间" json:"updated_at"`
}

// TableName 设置表名
func (DSLayerRecord) TableName() string {
	return "nw_ds_layer"
}

// 实现 JSON 序列化/反序列化的辅助类型
type JSONMap map[string]interface{}

// Scan implements the sql Scanner interface
func (j *JSONMap) Scan(value interface{}) error {
	if value == nil {
		*j = make(map[string]interface{})
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return nil
	}

	return json.Unmarshal(bytes, j)
}

// Value implements the sql Valuer interface
func (j JSONMap) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return json.Marshal(j)
}
