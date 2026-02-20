package treap

import (
	"errors"
	"fmt"
	"sort"
)

func flushOldestPercentileCommon[N any](
	percentage int,
	collectNodes func() ([]N, error),
	lastAccessTime func(N) int64,
	flushNode func(N) error,
) (int, error) {
	if percentage <= 0 || percentage > 100 {
		return 0, fmt.Errorf("percentage must be between 1 and 100, got %d", percentage)
	}

	nodes, err := collectNodes()
	if err != nil {
		return 0, err
	}
	if len(nodes) == 0 {
		return 0, nil
	}

	sort.Slice(nodes, func(i, j int) bool {
		return lastAccessTime(nodes[i]) < lastAccessTime(nodes[j])
	})

	numToFlush := (len(nodes) * percentage) / 100
	if numToFlush == 0 {
		numToFlush = 1
	}

	flushedCount := 0
	for i := 0; i < numToFlush && i < len(nodes); i++ {
		err := flushNode(nodes[i])
		if err != nil {
			if !errors.Is(err, errNotFullyPersisted) {
				return flushedCount, err
			}
			continue
		}
		flushedCount++
	}

	var zero N
	for i := range nodes {
		nodes[i] = zero
	}

	return flushedCount, nil
}
