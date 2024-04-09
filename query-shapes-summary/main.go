package main

import (
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
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

	if len(paths) != 1 {
		return errors.New("no path specified")
	}

	summaries, err := readRawData(paths[0])
	if err != nil {
		return err
	}

	return writeSummary(summaries)
}

func readRawData(path string) (map[string]*querySummary, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open '%v': %w", path, err)
	}

	defer f.Close()
	r := csv.NewReader(f)

	// Skip past the header.
	if _, err := r.Read(); err != nil {
		return nil, fmt.Errorf("could not read CSV header: %w", err)
	}

	summaries := map[string]*querySummary{}

	for {
		record, err := r.Read()
		if err == io.EOF {
			return summaries, nil
		}

		if err != nil {
			return nil, fmt.Errorf("could not read CSV: %w", err)
		}

		// Field order is: timestamp, original query, query type, cleaned query.
		queryType := record[2]
		cleanedQuery := record[3]

		if _, ok := summaries[cleanedQuery]; !ok {
			summaries[cleanedQuery] = &querySummary{}
		}

		summary := summaries[cleanedQuery]

		switch queryType {
		case "range":
			summary.rangeCount++
		case "instant":
			summary.instantCount++
		default:
			return nil, fmt.Errorf("unknown query type '%v'", queryType)
		}
	}
}

func writeSummary(summaries map[string]*querySummary) error {
	w := csv.NewWriter(os.Stdout)
	if err := w.Write([]string{"Cleaned query", "Range queries", "Instant queries"}); err != nil {
		return err
	}

	for query, summary := range summaries {
		if err := w.Write([]string{query, strconv.Itoa(summary.rangeCount), strconv.Itoa(summary.instantCount)}); err != nil {
			return err
		}
	}

	w.Flush()

	return w.Error()
}

type querySummary struct {
	instantCount int
	rangeCount   int
}
