package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/promql"
	"io"
	"log/slog"
	"math"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.Error("Application failed", "err", err)
		os.Exit(1)
	}
}

func run() error {
	flag.Parse()
	paths := flag.Args()

	if len(paths) == 0 {
		return errors.New("no paths specified")
	}

	stats := newStatistics()

	for _, path := range paths {
		slog.Info("Analysing file", "path", path)

		if err := analyseFile(path, stats); err != nil {
			return fmt.Errorf("analysing file %v failed: %w", path, err)
		}
	}

	slog.Info("Analysis complete")

	w := csv.NewWriter(os.Stdout)
	if err := w.Write([]string{"Range", "Select count"}); err != nil {
		return err
	}

	err := stats.ForBlockRanges(func(start time.Duration, count int64) error {
		return w.Write([]string{formatBlockDuration(start), strconv.FormatInt(count, 10)})
	})

	if err != nil {
		return err
	}

	if err := w.WriteAll([][]string{{"Total selects", strconv.FormatInt(stats.selectCount.Load(), 10)}, {"Total queries", strconv.FormatInt(stats.queryCount.Load(), 10)}}); err != nil {
		return err
	}

	w.Flush()

	return w.Error()
}

func formatBlockDuration(start time.Duration) string {
	return fmt.Sprintf("%v-%v", formatDuration(start), formatDuration(start+time.Hour))
}

func formatDuration(d time.Duration) string {
	days := math.Floor(d.Hours() / 24)
	hours := (d - time.Duration(days)*24*time.Hour).Hours()

	return fmt.Sprintf("%vd%vh", days, hours)
}

type statistics struct {
	queryCount  atomic.Int64
	selectCount atomic.Int64

	// Blocks queried.
	// Entry 0 is the "0-13h ago" block for queries to ingesters.
	// Entry 1 is the "12-24h ago" block.
	// Subsequent entries are for following 24h periods (24-48h, 48-72h, ...)
	blockRangesQueried []atomic.Int64
}

func newStatistics() *statistics {
	return &statistics{
		blockRangesQueried: make([]atomic.Int64, 396), // 13 months (395 days), but first day is split into 0-13h and 12-24h blocks.
	}
}

func (s *statistics) IncrementBlockRanges(from, to time.Duration) {
	if from > to {
		panic(fmt.Sprintf("from time (%v) after to time (%v)", from, to))
	}

	s.selectCount.Add(1)

	currentBlock := max(0, from)

	for currentBlock < to {
		i := currentBlock / time.Hour

		if int(i) >= len(s.blockRangesQueried) {
			// Reached the end of 365 day range. We're done.
			return
		}

		s.blockRangesQueried[i].Add(1)

		if currentBlock%(time.Hour) == 0 {
			// Already on a block boundary, advance to next block.
			currentBlock += time.Hour
		} else {
			// Not at a block boundary, advance to beginning of next block.
			currentBlock += time.Hour - (currentBlock % time.Hour)
		}
	}
}

func (s *statistics) ForBlockRanges(f func(start time.Duration, count int64) error) error {
	for i := range s.blockRangesQueried {
		start := time.Duration(i) * time.Hour

		if err := f(start, s.blockRangesQueried[i].Load()); err != nil {
			return err
		}
	}

	return nil
}

func analyseFile(path string, stats *statistics) error {
	f, err := os.Open(path)

	if err != nil {
		return fmt.Errorf("could not open file: %w", err)
	}

	defer f.Close()

	r := bufio.NewReader(f)

	for {
		l := ""

		for {
			portion, isPrefix, err := r.ReadLine()
			if err != nil {
				if err == io.EOF {
					return nil
				}

				return err
			}

			l += string(portion)

			if !isPrefix {
				break
			}
		}

		if err := parseAndAnalyseLogLine(l, stats); err != nil {
			return err
		}
	}
}

func parseAndAnalyseLogLine(line string, stats *statistics) error {
	logLine, skip, err := parseLogLine(line)

	if skip == true {
		return nil
	}

	if err != nil {
		return fmt.Errorf("parsing log line '%v' failed: %w", line, err)
	}

	return analyseLogLine(logLine, stats)
}

var engine = promql.NewEngine(promql.EngineOpts{
	Logger:        log.NewNopLogger(),
	LookbackDelta: 5 * time.Minute, // Default value.
	NoStepSubqueryIntervalFn: func(int64) int64 {
		return durationToInt64Millis(time.Duration(config.DefaultGlobalConfig.EvaluationInterval))
	},
	Timeout:    time.Minute,
	MaxSamples: math.MaxInt,

	// Default values as of Prometheus v2.33:
	EnableAtModifier:     true,
	EnableNegativeOffset: true,
})

var queryOpts = promql.NewPrometheusQueryOpts(false, 0)

func analyseLogLine(logLine logLine, stats *statistics) error {
	stats.queryCount.Add(1)

	queryable := &queryRangeCollectingQueryable{
		stats:          stats,
		queryTimestamp: logLine.timestamp,
	}

	var q promql.Query
	var err error

	if logLine.isRangeQuery {
		q, err = engine.NewRangeQuery(context.Background(), queryable, queryOpts, logLine.query, logLine.queryStartTime, logLine.queryEndTime, logLine.queryStep)
	} else {
		q, err = engine.NewInstantQuery(context.Background(), queryable, queryOpts, logLine.query, logLine.queryTime)
	}

	if err != nil {
		return fmt.Errorf("could not create query: %w", err)
	}

	defer q.Close()
	result := q.Exec(context.Background())

	if result.Err != nil {
		return fmt.Errorf("query execution failed: %w", result.Err)
	}

	return nil
}

func durationToInt64Millis(d time.Duration) int64 {
	return int64(d / time.Millisecond)
}
