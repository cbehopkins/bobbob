package store

import (
	"errors"
	"io"
)

// ErrWriteLimitExceeded is the error returned when the write limit is exceeded.
var ErrWriteLimitExceeded = errors.New("write limit exceeded")

// SectionWriter writes to a section of an underlying writer.
type SectionWriter struct {
	writer io.WriterAt
	offset int64
	limit  int64
	pos    int64
}

// NewSectionWriter creates a new SectionWriter.
func NewSectionWriter(writer io.WriterAt, offset, limit int64) *SectionWriter {
	return &SectionWriter{
		writer: writer,
		offset: offset,
		limit:  limit,
		pos:    0,
	}
}

// Write writes data to the underlying writer up to the limit.
// Returns ErrWriteLimitExceeded if the write would exceed the section boundary.
func (sw *SectionWriter) Write(p []byte) (int, error) {
	if sw.pos >= sw.limit {
		return 0, ErrWriteLimitExceeded
	}

	remaining := sw.limit - sw.pos
	if int64(len(p)) > remaining {
		return 0, ErrWriteLimitExceeded
	}

	n, err := sw.writer.WriteAt(p, sw.offset+sw.pos)
	sw.pos += int64(n)
	return n, err
}

// WriteAt writes data to the underlying writer at a specific offset within the section.
// Returns ErrWriteLimitExceeded if the write would exceed the section boundary.
func (sw *SectionWriter) WriteAt(p []byte, off int64) (int, error) {
	if off >= sw.limit {
		return 0, ErrWriteLimitExceeded
	}

	remaining := sw.limit - off
	if int64(len(p)) > remaining {
		return 0, ErrWriteLimitExceeded
	}

	n, err := sw.writer.WriteAt(p, sw.offset+off)
	return n, err
}

// BytesWritten returns the number of bytes written to this section so far.
func (sw *SectionWriter) BytesWritten() int64 {
	return sw.pos
}

// Limit returns the maximum number of bytes that can be written to this section.
func (sw *SectionWriter) Limit() int64 {
	return sw.limit
}
