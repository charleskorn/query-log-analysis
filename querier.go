package main

import (
	"context"
	"github.com/prometheus/prometheus/model/labels"
	"time"
)
import "github.com/prometheus/prometheus/storage"

type queryRangeCollectingQueryable struct {
	stats *statistics

	// The time the query was executed.
	queryTimestamp time.Time
}

func (q *queryRangeCollectingQueryable) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	// Note that min/max are swapped to from/to to match the meaning in the statistics type:
	// the time range is "`from` ago to `to` ago"
	from := q.queryTimestamp.Sub(int64MillisToTime(maxt))
	to := q.queryTimestamp.Sub(int64MillisToTime(mint))

	//fmt.Printf("Time range is %v ago to %v ago\n", from, to)

	return &queryRangeCollectingQuerier{stats: q.stats, from: from, to: to}, nil
}

func int64MillisToTime(i int64) time.Time {
	return time.UnixMilli(i)
}

var _ storage.Queryable = &queryRangeCollectingQueryable{}

type queryRangeCollectingQuerier struct {
	stats *statistics

	from, to time.Duration
}

func (q *queryRangeCollectingQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {

	q.stats.IncrementBlockRanges(q.from, q.to)

	return storage.EmptySeriesSet()
}

func (q *queryRangeCollectingQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (q *queryRangeCollectingQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (q *queryRangeCollectingQuerier) Close() error {
	// Nothing to do.
	return nil
}

var _ storage.Querier = &queryRangeCollectingQuerier{}
