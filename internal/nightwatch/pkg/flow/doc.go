// Package flow provides nightwatch-specific stream processing components built on top of pkg/streams.
// Instead of reimplementing stream processing functionality, this package leverages the existing
// pkg/streams framework and provides domain-specific adapters and utilities for batch processing.
//
// Key design principles:
// 1. Use pkg/streams as the foundation for all stream processing
// 2. Provide domain-specific Source, Flow, and Sink implementations
// 3. Avoid duplicating functionality that already exists in pkg/streams
// 4. Focus on nightwatch-specific business logic and data types
package flow
