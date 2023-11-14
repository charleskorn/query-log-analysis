package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/go-logfmt/logfmt"
	"github.com/prometheus/common/model"
)

type logLine struct {
	timestamp time.Time
	query     string

	isRangeQuery bool // If false, then this line represents an instant query

	// Instant query fields
	queryTime time.Time

	// Range query fields
	queryStartTime time.Time
	queryEndTime   time.Time
	queryStep      time.Duration
}

func parseLogLine(line string) (logLine, error) {
	jsonLine := struct {
		Line string `json:"line"`
	}{}

	if err := json.Unmarshal([]byte(line), &jsonLine); err != nil {
		return logLine{}, err
	}

	d := logfmt.NewDecoder(strings.NewReader(jsonLine.Line))
	parsed := logLine{}

	for d.ScanRecord() {
		for d.ScanKeyval() {
			value := string(d.Value())

			switch string(d.Key()) {
			case "ts":
				ts, err := time.Parse(time.RFC3339Nano, value)
				if err != nil {
					return logLine{}, fmt.Errorf("invalid log timestamp '%v': %w", value, err)
				}
				parsed.timestamp = ts

			case "param_query":
				parsed.query = value

			case "path":
				switch value {
				case "/prometheus/api/v1/query":
					parsed.isRangeQuery = false
				case "/prometheus/api/v1/query_range":
					parsed.isRangeQuery = true
				default:
					return logLine{}, fmt.Errorf("unknown path '%v'", value)
				}

			case "param_time":
				ts, err := parseTime(value)
				if err != nil {
					return logLine{}, fmt.Errorf("invalid query time '%v': %w", value, err)
				}
				parsed.queryTime = ts

			case "param_start":
				ts, err := parseTime(value)
				if err != nil {
					return logLine{}, fmt.Errorf("invalid query start time '%v': %w", value, err)
				}
				parsed.queryStartTime = ts

			case "param_end":
				ts, err := parseTime(value)
				if err != nil {
					return logLine{}, fmt.Errorf("invalid query end time '%v': %w", value, err)
				}
				parsed.queryEndTime = ts

			case "param_step":
				d, err := parseDuration(value)
				if err != nil {
					return logLine{}, fmt.Errorf("invalid query end time '%v': %w", value, err)
				}
				parsed.queryStep = d
			}
		}
	}

	if parsed.timestamp.IsZero() {
		return logLine{}, errors.New("no timestamp")
	}

	if parsed.query == "" {
		return logLine{}, errors.New("no query")
	}

	if parsed.isRangeQuery {
		if parsed.queryStartTime.IsZero() {
			return logLine{}, errors.New("no query start time for range query")
		}

		if parsed.queryEndTime.IsZero() {
			return logLine{}, errors.New("no query end time for range query")
		}

		if parsed.queryStep == 0 {
			return logLine{}, errors.New("no step for range query")
		}
	} else {
		if parsed.queryTime.IsZero() {
			return logLine{}, errors.New("no query time for instant query")
		}
	}

	return parsed, d.Err()
}

// From github.com/prometheus/prometheus/web/api/v1/api.go

var (
	// MinTime is the default timestamp used for the begin of optional time ranges.
	// Exposed to let downstream projects to reference it.
	MinTime = time.Unix(math.MinInt64/1000+62135596801, 0).UTC()

	// MaxTime is the default timestamp used for the end of optional time ranges.
	// Exposed to let downstream projects to reference it.
	MaxTime = time.Unix(math.MaxInt64/1000-62135596801, 999999999).UTC()

	minTimeFormatted = MinTime.Format(time.RFC3339Nano)
	maxTimeFormatted = MaxTime.Format(time.RFC3339Nano)
)

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		ns = math.Round(ns*1000) / 1000
		return time.Unix(int64(s), int64(ns*float64(time.Second))).UTC(), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}

	// Stdlib's time parser can only handle 4 digit years. As a workaround until
	// that is fixed we want to at least support our own boundary times.
	// Context: https://github.com/prometheus/client_golang/issues/614
	// Upstream issue: https://github.com/golang/go/issues/20555
	switch s {
	case minTimeFormatted:
		return MinTime, nil
	case maxTimeFormatted:
		return MaxTime, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}
