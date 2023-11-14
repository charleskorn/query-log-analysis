package main

import (
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestStatistics(t *testing.T) {
	s := newStatistics()

	// Query range in the future
	s.IncrementBlockRanges(-1*time.Hour, -10*time.Minute)
	requireBlockRangesQueried(t, s, map[time.Duration]int64{})

	// Query range before 12h cutoff
	s.IncrementBlockRanges(0, 11*time.Hour)
	requireBlockRangesQueried(t, s, map[time.Duration]int64{
		0: 1,
	})

	// Query range overlaps 12h cutoff
	s.IncrementBlockRanges(1*time.Hour, 14*time.Hour)
	requireBlockRangesQueried(t, s, map[time.Duration]int64{
		0:              2,
		12 * time.Hour: 1,
	})

	s.IncrementBlockRanges(1*time.Hour, 49*time.Hour)
	requireBlockRangesQueried(t, s, map[time.Duration]int64{
		0:              3,
		12 * time.Hour: 2,
		24 * time.Hour: 1,
		48 * time.Hour: 1,
	})

	s.IncrementBlockRanges(12*time.Hour, 49*time.Hour)
	requireBlockRangesQueried(t, s, map[time.Duration]int64{
		0:              4,
		12 * time.Hour: 3,
		24 * time.Hour: 2,
		48 * time.Hour: 2,
	})

	s.IncrementBlockRanges(24*time.Hour, 49*time.Hour)
	requireBlockRangesQueried(t, s, map[time.Duration]int64{
		0:              4,
		12 * time.Hour: 3,
		24 * time.Hour: 3,
		48 * time.Hour: 3,
	})

	s.IncrementBlockRanges(27*time.Hour, 49*time.Hour)
	requireBlockRangesQueried(t, s, map[time.Duration]int64{
		0:              4,
		12 * time.Hour: 3,
		24 * time.Hour: 4,
		48 * time.Hour: 4,
	})

	// Inside single block ranges
	s.IncrementBlockRanges(13*time.Hour, 20*time.Hour)
	requireBlockRangesQueried(t, s, map[time.Duration]int64{
		0:              4,
		12 * time.Hour: 4,
		24 * time.Hour: 4,
		48 * time.Hour: 4,
	})

	s.IncrementBlockRanges(25*time.Hour, 26*time.Hour)
	requireBlockRangesQueried(t, s, map[time.Duration]int64{
		0:              4,
		12 * time.Hour: 4,
		24 * time.Hour: 5,
		48 * time.Hour: 4,
	})

	// Beyond end of range
	s.IncrementBlockRanges(360*24*time.Hour, 370*24*time.Hour)
	requireBlockRangesQueried(t, s, map[time.Duration]int64{
		0:                    4,
		12 * time.Hour:       4,
		24 * time.Hour:       5,
		48 * time.Hour:       4,
		360 * 24 * time.Hour: 1,
		361 * 24 * time.Hour: 1,
		362 * 24 * time.Hour: 1,
		363 * 24 * time.Hour: 1,
		364 * 24 * time.Hour: 1,
	})
}

func TestQueryAnalysis(t *testing.T) {
	baseTimestamp := time.Date(2023, 11, 13, 9, 20, 0, 0, time.UTC)

	testCases := map[string]struct {
		input logLine

		expectedSelectCount        int64
		expectedBlockRangesQueried map[time.Duration]int64
	}{
		"single select, range query touching single block": {
			input: logLine{
				timestamp: baseTimestamp,
				query:     "metric{}",

				isRangeQuery:   true,
				queryStartTime: baseTimestamp.Add(-47 * time.Hour),
				queryEndTime:   baseTimestamp.Add(-46 * time.Hour),
				queryStep:      30 * time.Second,
			},
			expectedSelectCount: 1,
			expectedBlockRangesQueried: map[time.Duration]int64{
				24 * time.Hour: 1,
			},
		},
		"single select, range query touching multiple blocks": {
			input: logLine{
				timestamp: baseTimestamp,
				query:     "metric{}",

				isRangeQuery:   true,
				queryStartTime: baseTimestamp.Add(-49 * time.Hour),
				queryEndTime:   baseTimestamp.Add(-27 * time.Hour),
				queryStep:      30 * time.Second,
			},
			expectedSelectCount: 1,
			expectedBlockRangesQueried: map[time.Duration]int64{
				24 * time.Hour: 1,
				48 * time.Hour: 1,
			},
		},
		"single select, range query with range selector": {
			input: logLine{
				timestamp: baseTimestamp,
				query:     "rate(metric{}[2h])",

				isRangeQuery:   true,
				queryStartTime: baseTimestamp.Add(-48 * time.Hour),
				queryEndTime:   baseTimestamp.Add(-47 * time.Hour),
				queryStep:      30 * time.Second,
			},
			expectedSelectCount: 1,
			expectedBlockRangesQueried: map[time.Duration]int64{
				24 * time.Hour: 1,
				48 * time.Hour: 1,
			},
		},
		"single select, instant query": {
			input: logLine{
				timestamp: baseTimestamp,
				query:     "metric{}",

				isRangeQuery: false,
				queryTime:    baseTimestamp.Add(-47 * time.Hour),
			},
			expectedSelectCount: 1,
			expectedBlockRangesQueried: map[time.Duration]int64{
				24 * time.Hour: 1,
			},
		},
		"single select, instant query with range selector": {
			input: logLine{
				timestamp: baseTimestamp,
				query:     "rate(metric{}[2h])",

				isRangeQuery: false,
				queryTime:    baseTimestamp.Add(-47 * time.Hour),
			},
			expectedSelectCount: 1,
			expectedBlockRangesQueried: map[time.Duration]int64{
				24 * time.Hour: 1,
				48 * time.Hour: 1,
			},
		},
		"multiple selects": {
			input: logLine{
				timestamp: baseTimestamp,
				query:     "metric_A{} / metric_B{}",

				isRangeQuery:   true,
				queryStartTime: baseTimestamp.Add(-47 * time.Hour),
				queryEndTime:   baseTimestamp.Add(-46 * time.Hour),
				queryStep:      30 * time.Second,
			},
			expectedSelectCount: 2,
			expectedBlockRangesQueried: map[time.Duration]int64{
				24 * time.Hour: 2,
			},
		},
		"no selects": {
			input: logLine{
				timestamp: baseTimestamp,
				query:     "vector(1)",

				isRangeQuery:   true,
				queryStartTime: baseTimestamp.Add(-48 * time.Hour),
				queryEndTime:   baseTimestamp.Add(-47 * time.Hour),
				queryStep:      30 * time.Second,
			},
			expectedSelectCount:        0,
			expectedBlockRangesQueried: map[time.Duration]int64{},
		},
		"single select, for ingester query time range": {
			input: logLine{
				timestamp: baseTimestamp,
				query:     "metric{}",

				isRangeQuery:   true,
				queryStartTime: baseTimestamp.Add(-3 * time.Hour),
				queryEndTime:   baseTimestamp.Add(-1 * time.Hour),
				queryStep:      30 * time.Second,
			},
			expectedSelectCount: 1,
			expectedBlockRangesQueried: map[time.Duration]int64{
				0: 1,
			},
		},
		"single select, for ingester and store-gateway query time range": {
			input: logLine{
				timestamp: baseTimestamp,
				query:     "metric{}",

				isRangeQuery:   true,
				queryStartTime: baseTimestamp.Add(-15 * time.Hour),
				queryEndTime:   baseTimestamp.Add(-1 * time.Hour),
				queryStep:      30 * time.Second,
			},
			expectedSelectCount: 1,
			expectedBlockRangesQueried: map[time.Duration]int64{
				0:              1,
				12 * time.Hour: 1,
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			stats := newStatistics()

			require.NoError(t, analyseLogLine(testCase.input, stats))

			require.Equal(t, int64(1), stats.queryCount.Load())
			require.Equal(t, testCase.expectedSelectCount, stats.selectCount.Load())
			requireBlockRangesQueried(t, stats, testCase.expectedBlockRangesQueried)
		})
	}
}

func requireBlockRangesQueried(t *testing.T, stats *statistics, expected map[time.Duration]int64) {
	actual := map[time.Duration]int64{}

	_ = stats.ForBlockRanges(func(start time.Duration, actualCount int64) error {
		if actualCount != 0 {
			actual[start] = actualCount
		}
		return nil
	})

	require.Equal(t, expected, actual)
}
