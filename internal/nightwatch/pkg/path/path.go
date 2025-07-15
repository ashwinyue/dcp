package path

import (
	"fmt"
	"path/filepath"
)

// JobPath provides job-related path utilities
type JobPath struct{}

// Job is the global instance for job path operations
var Job = &JobPath{}

// JobDataName is the filename for job data
const JobDataName = "data.json"

// JobEmbeddedDataName is the filename for embedded data
const JobEmbeddedDataName = "embedded.json"

// JobResultName is the filename for job results
const JobResultName = "result.json"

// Path generates a path for a job with the given jobID and filename
func (jp *JobPath) Path(jobID, filename string) string {
	return filepath.Join("jobs", jobID, filename)
}

// DataPath generates a data path for a job
func (jp *JobPath) DataPath(jobID string) string {
	return jp.Path(jobID, JobDataName)
}

// EmbeddedDataPath generates an embedded data path for a job
func (jp *JobPath) EmbeddedDataPath(jobID string) string {
	return jp.Path(jobID, JobEmbeddedDataName)
}

// ResultPath generates a result path for a job
func (jp *JobPath) ResultPath(jobID string) string {
	return jp.Path(jobID, JobResultName)
}

// LogPath generates a log path for a job
func (jp *JobPath) LogPath(jobID string) string {
	return jp.Path(jobID, "log.txt")
}

// ModelPath generates a model path for a job
func (jp *JobPath) ModelPath(jobID string) string {
	return jp.Path(jobID, "model.bin")
}

// JobDir generates the directory path for a job
func (jp *JobPath) JobDir(jobID string) string {
	return filepath.Join("jobs", jobID)
}

// BatchPath generates a batch path with the given components
func BatchPath(components ...string) string {
	return filepath.Join(components...)
}

// WithJobID creates a path with job ID prefix
func WithJobID(jobID string, path string) string {
	return fmt.Sprintf("job-%s-%s", jobID, path)
}
