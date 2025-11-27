package store

import (
	"errors"
	"testing"
)

// mockWriterAt is a simple WriterAt implementation for testing.
type mockWriterAt struct {
	data       []byte
	writeError error
}

func newMockWriterAt(size int) *mockWriterAt {
	return &mockWriterAt{
		data: make([]byte, size),
	}
}

func (m *mockWriterAt) WriteAt(p []byte, off int64) (int, error) {
	if m.writeError != nil {
		return 0, m.writeError
	}
	if off < 0 || off > int64(len(m.data)) {
		return 0, errors.New("offset out of range")
	}
	n := copy(m.data[off:], p)
	return n, nil
}

func TestNewSectionWriter(t *testing.T) {
	writer := newMockWriterAt(100)
	sw := NewSectionWriter(writer, 10, 50)

	if sw == nil {
		t.Fatal("expected NewSectionWriter to return a non-nil SectionWriter")
	}
	if sw.offset != 10 {
		t.Errorf("expected offset to be 10, got %d", sw.offset)
	}
	if sw.limit != 50 {
		t.Errorf("expected limit to be 50, got %d", sw.limit)
	}
	if sw.pos != 0 {
		t.Errorf("expected initial position to be 0, got %d", sw.pos)
	}
	if sw.writer != writer {
		t.Error("expected writer to be set correctly")
	}
}

func TestSectionWriterWrite(t *testing.T) {
	writer := newMockWriterAt(100)
	sw := NewSectionWriter(writer, 10, 50)

	// Write some data
	data := []byte("hello world")
	n, err := sw.Write(data)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if n != len(data) {
		t.Errorf("expected to write %d bytes, wrote %d", len(data), n)
	}
	if sw.pos != int64(len(data)) {
		t.Errorf("expected position to be %d, got %d", len(data), sw.pos)
	}

	// Verify data was written at the correct offset
	expected := "hello world"
	actual := string(writer.data[10:21])
	if actual != expected {
		t.Errorf("expected data to be %q, got %q", expected, actual)
	}
}

func TestSectionWriterWriteMultiple(t *testing.T) {
	writer := newMockWriterAt(100)
	sw := NewSectionWriter(writer, 20, 30)

	// First write
	n1, err := sw.Write([]byte("first"))
	if err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	if n1 != 5 {
		t.Errorf("expected to write 5 bytes, wrote %d", n1)
	}

	// Second write
	n2, err := sw.Write([]byte(" second"))
	if err != nil {
		t.Fatalf("second write failed: %v", err)
	}
	if n2 != 7 {
		t.Errorf("expected to write 7 bytes, wrote %d", n2)
	}

	if sw.pos != 12 {
		t.Errorf("expected position to be 12, got %d", sw.pos)
	}

	// Verify combined data
	expected := "first second"
	actual := string(writer.data[20:32])
	if actual != expected {
		t.Errorf("expected data to be %q, got %q", expected, actual)
	}
}

func TestSectionWriterWriteExceedsLimit(t *testing.T) {
	writer := newMockWriterAt(100)
	sw := NewSectionWriter(writer, 10, 20)

	// Try to write more data than the limit allows
	data := []byte("this is way too much data for the limit")
	n, err := sw.Write(data)
	if err != ErrWriteLimitExceeded {
		t.Errorf("expected ErrWriteLimitExceeded, got %v", err)
	}
	if n != 0 {
		t.Errorf("expected to write 0 bytes, wrote %d", n)
	}
	if sw.pos != 0 {
		t.Errorf("expected position to remain 0, got %d", sw.pos)
	}
}

func TestSectionWriterWriteAtLimit(t *testing.T) {
	writer := newMockWriterAt(100)
	sw := NewSectionWriter(writer, 10, 20)

	// Write exactly to the limit
	data := make([]byte, 20)
	for i := range data {
		data[i] = byte('A' + i)
	}

	n, err := sw.Write(data)
	if err != nil {
		t.Fatalf("expected no error writing at limit, got %v", err)
	}
	if n != 20 {
		t.Errorf("expected to write 20 bytes, wrote %d", n)
	}
	if sw.pos != 20 {
		t.Errorf("expected position to be 20, got %d", sw.pos)
	}

	// Try to write one more byte
	n, err = sw.Write([]byte("X"))
	if err != ErrWriteLimitExceeded {
		t.Errorf("expected ErrWriteLimitExceeded after reaching limit, got %v", err)
	}
	if n != 0 {
		t.Errorf("expected to write 0 bytes after limit, wrote %d", n)
	}
}

func TestSectionWriterWritePartialExceedsLimit(t *testing.T) {
	writer := newMockWriterAt(100)
	sw := NewSectionWriter(writer, 10, 20)

	// Write some data first
	n, err := sw.Write([]byte("12345678901234"))
	if err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	if n != 14 {
		t.Errorf("expected to write 14 bytes, wrote %d", n)
	}

	// Try to write data that would exceed the remaining limit
	n, err = sw.Write([]byte("1234567"))
	if err != ErrWriteLimitExceeded {
		t.Errorf("expected ErrWriteLimitExceeded, got %v", err)
	}
	if n != 0 {
		t.Errorf("expected to write 0 bytes on overflow, wrote %d", n)
	}
	if sw.pos != 14 {
		t.Errorf("expected position to remain 14, got %d", sw.pos)
	}
}

func TestSectionWriterWriteAt(t *testing.T) {
	writer := newMockWriterAt(100)
	sw := NewSectionWriter(writer, 10, 50)

	// Write at offset 5 within the section
	data := []byte("testing")
	n, err := sw.WriteAt(data, 5)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if n != len(data) {
		t.Errorf("expected to write %d bytes, wrote %d", len(data), n)
	}

	// Verify data was written at the correct absolute offset (10 + 5 = 15)
	expected := "testing"
	actual := string(writer.data[15:22])
	if actual != expected {
		t.Errorf("expected data to be %q, got %q", expected, actual)
	}

	// WriteAt should not affect pos
	if sw.pos != 0 {
		t.Errorf("expected position to remain 0, got %d", sw.pos)
	}
}

func TestSectionWriterWriteAtMultipleOffsets(t *testing.T) {
	writer := newMockWriterAt(100)
	sw := NewSectionWriter(writer, 20, 40)

	// Write at different offsets
	sw.WriteAt([]byte("AAA"), 0)
	sw.WriteAt([]byte("BBB"), 10)
	sw.WriteAt([]byte("CCC"), 20)

	// Verify data at each offset
	if string(writer.data[20:23]) != "AAA" {
		t.Errorf("expected AAA at offset 20, got %q", string(writer.data[20:23]))
	}
	if string(writer.data[30:33]) != "BBB" {
		t.Errorf("expected BBB at offset 30, got %q", string(writer.data[30:33]))
	}
	if string(writer.data[40:43]) != "CCC" {
		t.Errorf("expected CCC at offset 40, got %q", string(writer.data[40:43]))
	}
}

func TestSectionWriterWriteAtExceedsLimit(t *testing.T) {
	writer := newMockWriterAt(100)
	sw := NewSectionWriter(writer, 10, 20)

	// Try to write at an offset that would exceed the limit
	data := []byte("too much data")
	n, err := sw.WriteAt(data, 15)
	if err != ErrWriteLimitExceeded {
		t.Errorf("expected ErrWriteLimitExceeded, got %v", err)
	}
	if n != 0 {
		t.Errorf("expected to write 0 bytes, wrote %d", n)
	}
}

func TestSectionWriterWriteAtOffsetAtLimit(t *testing.T) {
	writer := newMockWriterAt(100)
	sw := NewSectionWriter(writer, 10, 20)

	// Try to write at offset equal to limit
	data := []byte("data")
	n, err := sw.WriteAt(data, 20)
	if err != ErrWriteLimitExceeded {
		t.Errorf("expected ErrWriteLimitExceeded, got %v", err)
	}
	if n != 0 {
		t.Errorf("expected to write 0 bytes, wrote %d", n)
	}
}

func TestSectionWriterWriteAtOffsetBeyondLimit(t *testing.T) {
	writer := newMockWriterAt(100)
	sw := NewSectionWriter(writer, 10, 20)

	// Try to write at offset beyond limit
	data := []byte("data")
	n, err := sw.WriteAt(data, 25)
	if err != ErrWriteLimitExceeded {
		t.Errorf("expected ErrWriteLimitExceeded, got %v", err)
	}
	if n != 0 {
		t.Errorf("expected to write 0 bytes, wrote %d", n)
	}
}

func TestSectionWriterWriteAtZeroLength(t *testing.T) {
	writer := newMockWriterAt(100)
	sw := NewSectionWriter(writer, 10, 20)

	// Write zero-length data
	n, err := sw.WriteAt([]byte{}, 5)
	if err != nil {
		t.Errorf("expected no error writing zero bytes, got %v", err)
	}
	if n != 0 {
		t.Errorf("expected to write 0 bytes, wrote %d", n)
	}
}

func TestSectionWriterWriteZeroLength(t *testing.T) {
	writer := newMockWriterAt(100)
	sw := NewSectionWriter(writer, 10, 20)

	// Write zero-length data
	n, err := sw.Write([]byte{})
	if err != nil {
		t.Errorf("expected no error writing zero bytes, got %v", err)
	}
	if n != 0 {
		t.Errorf("expected to write 0 bytes, wrote %d", n)
	}
	if sw.pos != 0 {
		t.Errorf("expected position to remain 0, got %d", sw.pos)
	}
}

func TestSectionWriterUnderlyingWriterError(t *testing.T) {
	writer := newMockWriterAt(100)
	writer.writeError = errors.New("mock write error")
	sw := NewSectionWriter(writer, 10, 20)

	// Try to write when underlying writer returns error
	data := []byte("test")
	n, err := sw.Write(data)
	if err == nil {
		t.Error("expected error from underlying writer, got nil")
	}
	if err.Error() != "mock write error" {
		t.Errorf("expected 'mock write error', got %v", err)
	}
	if n != 0 {
		t.Errorf("expected to write 0 bytes on error, wrote %d", n)
	}
}

func TestSectionWriterUnderlyingWriterErrorWriteAt(t *testing.T) {
	writer := newMockWriterAt(100)
	writer.writeError = errors.New("mock write error")
	sw := NewSectionWriter(writer, 10, 20)

	// Try to write when underlying writer returns error
	data := []byte("test")
	n, err := sw.WriteAt(data, 5)
	if err == nil {
		t.Error("expected error from underlying writer, got nil")
	}
	if err.Error() != "mock write error" {
		t.Errorf("expected 'mock write error', got %v", err)
	}
	if n != 0 {
		t.Errorf("expected to write 0 bytes on error, wrote %d", n)
	}
}

func TestSectionWriterMixedWriteAndWriteAt(t *testing.T) {
	writer := newMockWriterAt(100)
	sw := NewSectionWriter(writer, 10, 50)

	// Use Write to write sequentially
	sw.Write([]byte("sequential"))
	if sw.pos != 10 {
		t.Errorf("expected position to be 10, got %d", sw.pos)
	}

	// Use WriteAt to write at a specific offset (doesn't affect pos)
	sw.WriteAt([]byte("random"), 20)
	if sw.pos != 10 {
		t.Errorf("expected position to still be 10, got %d", sw.pos)
	}

	// Continue sequential write
	sw.Write([]byte("_more"))
	if sw.pos != 15 {
		t.Errorf("expected position to be 15, got %d", sw.pos)
	}

	// Verify the data
	if string(writer.data[10:20]) != "sequential" {
		t.Errorf("unexpected sequential data: %q", string(writer.data[10:20]))
	}
	if string(writer.data[30:36]) != "random" {
		t.Errorf("unexpected random access data: %q", string(writer.data[30:36]))
	}
	if string(writer.data[20:25]) != "_more" {
		t.Errorf("unexpected continued sequential data: %q", string(writer.data[20:25]))
	}
}

func TestSectionWriterBoundaries(t *testing.T) {
	writer := newMockWriterAt(100)
	
	tests := []struct {
		name   string
		offset int64
		limit  int64
	}{
		{"zero offset", 0, 50},
		{"mid offset", 50, 30},
		{"near end", 90, 10},
		{"single byte section", 50, 1},
		{"zero limit", 50, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sw := NewSectionWriter(writer, tt.offset, tt.limit)
			if sw.offset != tt.offset {
				t.Errorf("expected offset %d, got %d", tt.offset, sw.offset)
			}
			if sw.limit != tt.limit {
				t.Errorf("expected limit %d, got %d", tt.limit, sw.limit)
			}
		})
	}
}

func TestSectionWriterZeroLimit(t *testing.T) {
	writer := newMockWriterAt(100)
	sw := NewSectionWriter(writer, 10, 0)

	// Any write should fail with zero limit
	n, err := sw.Write([]byte("X"))
	if err != ErrWriteLimitExceeded {
		t.Errorf("expected ErrWriteLimitExceeded with zero limit, got %v", err)
	}
	if n != 0 {
		t.Errorf("expected to write 0 bytes, wrote %d", n)
	}

	// WriteAt should also fail
	n, err = sw.WriteAt([]byte("X"), 0)
	if err != ErrWriteLimitExceeded {
		t.Errorf("expected ErrWriteLimitExceeded with zero limit, got %v", err)
	}
	if n != 0 {
		t.Errorf("expected to write 0 bytes, wrote %d", n)
	}
}
