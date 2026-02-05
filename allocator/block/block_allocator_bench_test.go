package block

import (
	"os"
	"testing"

	"github.com/cbehopkins/bobbob/allocator/types"
)

// BenchmarkAllocate_Empty measures allocation performance on an empty allocator
func BenchmarkAllocate_Empty(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096}
	counts := []int{1000, 10000, 100000}

	for _, size := range sizes {
		for _, count := range counts {
			b.Run(formatName(size, count, 0), func(b *testing.B) {
				benchAllocateAtFillRate(b, size, count, 0.0)
			})
		}
	}
}

// BenchmarkAllocate_HalfFull measures allocation when 50% full
func BenchmarkAllocate_HalfFull(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096}
	counts := []int{1000, 10000, 100000}

	for _, size := range sizes {
		for _, count := range counts {
			b.Run(formatName(size, count, 50), func(b *testing.B) {
				benchAllocateAtFillRate(b, size, count, 0.5)
			})
		}
	}
}

// BenchmarkAllocate_NearlyFull measures allocation when 90% full
func BenchmarkAllocate_NearlyFull(b *testing.B) {
	sizes := []int{64, 256, 1024, 4096}
	counts := []int{1000, 10000, 100000}

	for _, size := range sizes {
		for _, count := range counts {
			b.Run(formatName(size, count, 90), func(b *testing.B) {
				benchAllocateAtFillRate(b, size, count, 0.9)
			})
		}
	}
}

// BenchmarkAllocate_AlmostFull measures allocation when 99% full
func BenchmarkAllocate_AlmostFull(b *testing.B) {
	sizes := []int{64, 256, 1024}
	counts := []int{1000, 10000, 100000}

	for _, size := range sizes {
		for _, count := range counts {
			b.Run(formatName(size, count, 99), func(b *testing.B) {
				benchAllocateAtFillRate(b, size, count, 0.99)
			})
		}
	}
}

// BenchmarkAllocate_WithCallback measures overhead of allocation callbacks
func BenchmarkAllocate_WithCallback(b *testing.B) {
	size := 1024
	count := 10000
	fillRate := 0.5

	b.Run("NoCallback", func(b *testing.B) {
		benchAllocateAtFillRate(b, size, count, fillRate)
	})

	b.Run("WithCallback", func(b *testing.B) {
		file, err := os.CreateTemp("", "block_allocator_bench_*.dat")
		if err != nil {
			b.Fatal(err)
		}
		defer os.Remove(file.Name())
		defer file.Close()

		a := New(size, count, 0, 0, file)

		// Add a simple callback
		a.SetOnAllocate(func(objId types.ObjectId, offset types.FileOffset, sz int) {
			// Minimal work to simulate callback overhead
			_ = objId + types.ObjectId(sz)
		})

		// Pre-fill allocator
		toFill := int(float64(count) * fillRate)
		for i := 0; i < toFill; i++ {
			_, _, _ = a.Allocate(size)
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_, _, err := a.Allocate(size)
			if err != nil {
				// Allocator full, reset
				b.StopTimer()
				a = New(size, count, 0, 0, file)
				a.SetOnAllocate(func(objId types.ObjectId, offset types.FileOffset, sz int) {
					_ = objId + types.ObjectId(sz)
				})
				for j := 0; j < toFill; j++ {
					_, _, _ = a.Allocate(size)
				}
				b.StartTimer()
				_, _, _ = a.Allocate(size)
			}
		}
	})
}

// BenchmarkAllocate_LinearScan isolates the bitmap scanning overhead
func BenchmarkAllocate_LinearScan(b *testing.B) {
	counts := []int{1000, 10000, 100000, 1000000}

	for _, count := range counts {
		// Removed 99% as it is very slow for linear scan
		for _, fillPercent := range []int{0, 50, 90, 95} {
			b.Run(formatScanName(count, fillPercent), func(b *testing.B) {
				file, err := os.CreateTemp("", "block_allocator_bench_*.dat")
				if err != nil {
					b.Fatal(err)
				}
				defer os.Remove(file.Name())
				defer file.Close()

				size := 1024
				a := New(size, count, 0, 0, file)

				// Pre-fill to specific percentage
				toFill := (count * fillPercent) / 100
				for i := 0; i < toFill; i++ {
					_, _, _ = a.Allocate(size)
				}

				b.ResetTimer()
				b.ReportAllocs()

				successCount := 0
				for i := 0; i < b.N; i++ {
					_, _, err := a.Allocate(size)
					if err == nil {
						successCount++
						// When full, reset and refill
						if successCount >= (count - toFill) {
							b.StopTimer()
							a = New(size, count, 0, 0, file)
							for j := 0; j < toFill; j++ {
								_, _, _ = a.Allocate(size)
							}
							successCount = 0
							b.StartTimer()
						}
					}
				}
			})
		}
	}
}

// BenchmarkFreeCountUpdate measures the cost of maintaining freeCount
func BenchmarkFreeCountUpdate(b *testing.B) {
	file, err := os.CreateTemp("", "block_allocator_bench_*.dat")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	size := 1024
	count := 10000
	a := New(size, count, 0, 0, file)

	// Pre-fill 50%
	toFill := count / 2
	for i := 0; i < toFill; i++ {
		_, _, _ = a.Allocate(size)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, _ = a.Allocate(size)
		if a.allAllocated {
			// Reset
			b.StopTimer()
			a = New(size, count, 0, 0, file)
			for j := 0; j < toFill; j++ {
				_, _, _ = a.Allocate(size)
			}
			b.StartTimer()
		}
	}
}

// Helper function to create consistent benchmark names
func formatName(size, count, fillPercent int) string {
	if fillPercent == 0 {
		return formatScanName(count, fillPercent)
	}
	return formatScanName(count, fillPercent)
}

func formatScanName(count, fillPercent int) string {
	switch count {
	case 1000:
		return formatFillName("1K", fillPercent)
	case 10000:
		return formatFillName("10K", fillPercent)
	case 100000:
		return formatFillName("100K", fillPercent)
	case 1000000:
		return formatFillName("1M", fillPercent)
	default:
		return formatFillName("Custom", fillPercent)
	}
}

func formatFillName(countStr string, fillPercent int) string {
	switch fillPercent {
	case 0:
		return countStr + "_Empty"
	case 50:
		return countStr + "_50pct"
	case 90:
		return countStr + "_90pct"
	case 95:
		return countStr + "_95pct"
	case 99:
		return countStr + "_99pct"
	default:
		return countStr + "_Custom"
	}
}

// benchAllocateAtFillRate is a helper for running allocation benchmarks at various fill rates
func benchAllocateAtFillRate(b *testing.B, size, count int, fillRate float64) {
	file, err := os.CreateTemp("", "block_allocator_bench_*.dat")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	a := New(size, count, 0, 0, file)

	// Pre-fill allocator to desired rate
	toFill := int(float64(count) * fillRate)
	for i := 0; i < toFill; i++ {
		_, _, _ = a.Allocate(size)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _, err := a.Allocate(size)
		if err != nil {
			// Allocator full, reset for next iteration
			b.StopTimer()
			a = New(size, count, 0, 0, file)
			for j := 0; j < toFill; j++ {
				_, _, _ = a.Allocate(size)
			}
			b.StartTimer()
			_, _, _ = a.Allocate(size)
		}
	}
}
