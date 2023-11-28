package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"github.com/prometheus/prometheus/promql/parser"
	"io"
	"log/slog"
	"os"
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

	var results []queryInfo

	for _, path := range paths {
		slog.Info("Analysing file", "path", path)

		fileResults, err := analyseFile(path)

		if err != nil {
			return fmt.Errorf("analysing file %v failed: %w", path, err)
		}

		results = append(results, fileResults...)
	}

	slog.Info("Analysis complete")

	w := csv.NewWriter(os.Stdout)
	if err := w.Write([]string{"Timestamp", "Original query", "Query type", "Cleaned query"}); err != nil {
		return err
	}

	for _, q := range results {
		if err := w.Write([]string{q.timestamp.Format(time.RFC3339Nano), q.originalQuery, q.queryType, q.cleanedQuery}); err != nil {
			return err
		}
	}

	w.Flush()

	return w.Error()
}

type queryInfo struct {
	timestamp     time.Time
	originalQuery string
	queryType     string // "instant" or "range"
	cleanedQuery  string
}

func analyseFile(path string) ([]queryInfo, error) {
	f, err := os.Open(path)

	if err != nil {
		return nil, fmt.Errorf("could not open file: %w", err)
	}

	defer f.Close()

	r := bufio.NewReader(f)
	lineNumber := 0
	var results []queryInfo

	for {
		l := ""

		for {
			portion, isPrefix, err := r.ReadLine()
			if err != nil {
				if err == io.EOF {
					return results, nil
				}

				return nil, err
			}

			l += string(portion)

			if !isPrefix {
				break
			}
		}

		lineNumber++
		result, skip, err := parseAndAnalyseLogLine(l)

		if err != nil {
			return nil, fmt.Errorf("line %v: %w", lineNumber, err)
		}

		if !skip {
			results = append(results, result)
		}
	}
}

func parseAndAnalyseLogLine(line string) (queryInfo, bool, error) {
	logLine, skip, err := parseLogLine(line)

	if skip == true {
		return queryInfo{}, true, nil
	}

	if err != nil {
		return queryInfo{}, false, fmt.Errorf("parsing log line '%v' failed: %w", line, err)
	}

	info, err := analyseLogLine(logLine)
	return info, false, err
}

func analyseLogLine(logLine logLine) (queryInfo, error) {
	p := parser.NewParser(logLine.query)
	defer p.Close()
	expr, err := p.ParseExpr()

	if err != nil {
		return queryInfo{}, fmt.Errorf("could not parse query '%s': %w", logLine.query, err)
	}

	if err := cleanExpr(expr); err != nil {
		return queryInfo{}, fmt.Errorf("could not clean query '%s': %w", logLine.query, err)
	}

	info := queryInfo{
		timestamp:     logLine.timestamp,
		originalQuery: logLine.query,
		cleanedQuery:  expr.String(),
	}

	if logLine.isRangeQuery {
		info.queryType = "range"
	} else {
		info.queryType = "instant"
	}

	return info, nil
}

func cleanExpr(expr parser.Expr) error {
	switch e := expr.(type) {
	case nil:
		return nil

	case *parser.AggregateExpr:
		if err := cleanExpr(e.Expr); err != nil {
			return err
		}

		if err := cleanExpr(e.Param); err != nil {
			return err
		}

		if len(e.Grouping) > 0 {
			e.Grouping = []string{"labels"}
		}

		return nil

	case *parser.BinaryExpr:
		if err := cleanExpr(e.LHS); err != nil {
			return err
		}

		if err := cleanExpr(e.RHS); err != nil {
			return err
		}

		if e.VectorMatching != nil {
			if len(e.VectorMatching.MatchingLabels) > 0 {
				e.VectorMatching.MatchingLabels = []string{"labels"}
			}

			if len(e.VectorMatching.Include) > 0 {
				e.VectorMatching.Include = []string{"labels"}
			}
		}

		return nil

	case *parser.Call:
		for _, arg := range e.Args {
			if err := cleanExpr(arg); err != nil {
				return err
			}
		}

		return nil

	case *parser.MatrixSelector:
		e.Range = time.Minute
		return cleanExpr(e.VectorSelector)

	case *parser.SubqueryExpr:
		if e.Timestamp != nil {
			t := int64(123)
			e.Timestamp = &t
		}

		// TODO: offset (either timestamp, start() or end())

		e.Step = time.Minute
		e.Range = time.Hour

		return cleanExpr(e.Expr)

	case *parser.NumberLiteral:
		e.Val = 123
		return nil

	case *parser.ParenExpr:
		return cleanExpr(e.Expr)

	case *parser.StringLiteral:
		e.Val = "abc"
		return nil

	case *parser.UnaryExpr:
		return cleanExpr(e.Expr)

	case *parser.StepInvariantExpr:
		return cleanExpr(e.Expr)

	case *parser.VectorSelector:
		// TODO: timestamp, offset (either timestamp, start() or end())

		e.Name = "metric"
		e.LabelMatchers = nil

		return nil

	default:
		return fmt.Errorf("unknown expression type %T", expr)
	}
}
