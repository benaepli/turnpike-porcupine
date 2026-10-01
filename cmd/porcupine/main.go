// Command porcupine checks client histories against the specification's
// consistency claim and draws each history as an HTML page.
//
// Exit codes:
//
//	0 - every history satisfies the claim
//	1 - usage / IO / query error
//	2 - at least one history violates the claim
//	4 - no violation, but at least one history is undecided
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gocarina/gocsv"

	"github.com/benaepli/turnpike-porcupine/checker"
	"github.com/benaepli/turnpike-porcupine/checker/engine"
	"github.com/benaepli/turnpike-porcupine/checker/view"
	"github.com/benaepli/turnpike-porcupine/stats"
)

type settings struct {
	claim   engine.Claim
	timeout time.Duration
}

func main() {
	inputFile := flag.String("input", "", "Path to the input file (CSV or DuckDB database) (required)")
	inputType := flag.String("type", "duckdb", "Input type: 'csv' or 'duckdb' (default: duckdb)")
	runID := flag.Int("run", -1, "Run ID to check (DuckDB only; -1 means all runs)")
	outputFile := flag.String("output", "", "Path for output HTML file (single run) or directory (all runs)")
	outputDir := flag.String("output-dir", "", "Output directory for HTML files (when processing all runs)")
	modelName := flag.String("model", "", "Model to check: kv|kv_rmw (default: the model recorded in the deployments table; required for CSV)")
	claimFlag := flag.String("claim", "", `Claim to check, as its JSON object, e.g. {"Read":"sequential","Write":"linearizable"} (default: the claim recorded in the deployments table, else every operation linearizable)`)
	timeoutMs := flag.Int("timeout", 0, "Per-stage check timeout in milliseconds (0 = no timeout)")
	recheckAll := flag.Bool("recheck-all", false, "Recheck every history and generate all HTML reports")
	flag.Parse()

	if *inputFile == "" {
		flag.Usage()
		log.Fatalln("Error: -input flag is required.")
	}

	inputTypeNorm := strings.ToLower(*inputType)
	if inputTypeNorm != "csv" && inputTypeNorm != "duckdb" {
		log.Fatalf("invalid input type %q (use csv|duckdb)", *inputType)
	}

	// A CSV history carries no deployments table to read the model from.
	if inputTypeNorm == "csv" && *modelName == "" {
		flag.Usage()
		log.Fatalln("Error: -model flag is required for CSV input.")
	}
	claimSource := ""
	if inputTypeNorm == "duckdb" {
		resolved, warning, err := checker.ResolveModel(*modelName, *inputFile)
		if err != nil {
			log.Fatalf("Error: %v", err)
		}
		if warning != "" {
			log.Printf("Warning: %s", warning)
		}
		if warning := checker.SymbolicWarning(*inputFile); warning != "" {
			log.Printf("Warning: %s", warning)
		}
		*modelName = resolved
		claimSource = *inputFile
	}

	model, err := engine.ParseModel(*modelName)
	if err != nil {
		log.Fatalf("unknown model %q (use kv|kv_rmw)", *modelName)
	}
	claim, warning, err := checker.ResolveClaim(*claimFlag, model, claimSource)
	if err != nil {
		log.Fatalf("Error: %v", err)
	}
	if warning != "" {
		log.Printf("Warning: %s", warning)
	}
	fmt.Printf("Model %s, claim %s %s\n", model, claim.Name(), claim.JSON())
	s := settings{claim: claim, timeout: time.Duration(*timeoutMs) * time.Millisecond}

	if inputTypeNorm == "csv" {
		if *outputFile == "" {
			log.Fatalln("Error: -output flag is required for CSV mode.")
		}
		processCSV(*inputFile, *outputFile, s)
		return
	}
	if *runID == -1 {
		outDir := *outputDir
		if outDir == "" {
			outDir = *outputFile
		}
		processAllRuns(*inputFile, outDir, s, *recheckAll)
		return
	}
	if *outputFile == "" {
		log.Fatalln("Error: -output flag is required when checking a single run.")
	}
	processSingleRun(*inputFile, *runID, *outputFile, s)
}

func processCSV(inputFile, outputFile string, s settings) {
	f, err := os.Open(inputFile)
	if err != nil {
		log.Fatalf("failed to open input file %s: %v", inputFile, err)
	}
	defer func(f *os.File) {
		_ = f.Close()
	}(f)

	var eventRows []*checker.EventRow
	if err := gocsv.UnmarshalFile(f, &eventRows); err != nil {
		log.Fatalf("failed to unmarshal CSV: %v", err)
	}
	h := check(eventRows, "CSV", s)
	report(h, h.outcome, outputFile, nil, s)
	exitForVerdict(h.outcome.Verdict)
}

func processSingleRun(dbPath string, runID int, outputFile string, s settings) {
	cache, err := checker.LoadCheckCache(dbPath, s.claim.Model.String(), s.claim.JSON())
	if err != nil {
		log.Fatalf("failed to read check metadata: %v", err)
	}
	eventRows, err := checker.ReadEventsFromDuckDB(dbPath, runID)
	if err != nil {
		log.Fatalf("failed to read events from DuckDB: %v", err)
	}
	h := check(eventRows, fmt.Sprintf("Run %d", runID), s)
	outcome := h.outcome
	if cached, ok := cache.Runs[runID]; ok {
		outcome, err = checker.ReconcileVerdict(runID, outcome, &cached)
		if err != nil {
			log.Fatal(err)
		}
	}
	report(h, outcome, outputFile, readTopology(dbPath).NamesForRun(runID), s)
	exitForVerdict(outcome.Verdict)
}

func processAllRuns(dbPath, outputDir string, s settings, recheckAll bool) {
	if outputDir != "" {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			log.Fatalf("failed to create output directory %s: %v", outputDir, err)
		}
	}

	violations, unknown, reused := 0, 0, 0
	runCount := 0
	triage := map[string]int{}
	var results []stats.RunResult
	topology := readTopology(dbPath)

	err := checker.ProcessRunsWithCache(dbPath, s.claim.Model.String(), s.claim.JSON(), recheckAll, func(runID int, eventRows []*checker.EventRow, cached *checker.CachedVerdict, reuse bool) error {
		runCount++
		if reuse {
			reused++
			triage[cached.Triage]++
			return nil
		}
		outFile := filepath.Join(outputDir, fmt.Sprintf("run_%d.html", runID))

		fmt.Printf("\n=== Checking Run %d ===\n", runID)
		start := time.Now()
		h := check(eventRows, fmt.Sprintf("Run %d", runID), s)
		outcome, err := checker.ReconcileVerdict(runID, h.outcome, cached)
		if err != nil {
			return err
		}
		elapsed := time.Since(start)
		report(h, outcome, outFile, topology.NamesForRun(runID), s)

		results = append(results, stats.RunResult{
			FileName:     fmt.Sprintf("run_%d", runID),
			ElapsedTime:  elapsed,
			Success:      outcome.Verdict != engine.VerdictUnknown,
			Linearizable: outcome.Verdict == engine.VerdictOK,
		})
		triage[outcome.Triage]++
		switch outcome.Verdict {
		case engine.VerdictIllegal:
			violations++
		case engine.VerdictUnknown:
			unknown++
		}
		return nil
	})
	if err != nil {
		log.Fatalf("failed to process runs: %v", err)
	}

	if runCount == 0 {
		log.Println("No runs found in database.")
		return
	}

	if len(results) > 0 {
		fmt.Printf("\nTiming for %d newly checked runs:\n", len(results))
		stats.PrintSummary(stats.CalculateStats(results))
	}

	slowThreshold := 5 * time.Second
	var slowRuns []stats.RunResult
	for _, r := range results {
		if r.ElapsedTime > slowThreshold {
			slowRuns = append(slowRuns, r)
		}
	}
	if len(slowRuns) > 0 {
		fmt.Printf("\n=== Slow Runs (>%v) ===\n", slowThreshold)
		for _, r := range slowRuns {
			fmt.Printf("  %s: %v (claim holds: %v)\n", r.FileName, r.ElapsedTime.Round(time.Millisecond), r.Linearizable)
		}
	}

	fmt.Printf("\n=== Summary (%d runs, claim %s) ===\n", runCount, s.claim.Name())
	fmt.Printf("%d reused passes (HTML omitted), %d violations, %d unknown\n", reused, violations, unknown)
	fmt.Printf("triage: %s\n", triageText(triage))
	if violations > 0 {
		os.Exit(2)
	}
	if unknown > 0 {
		os.Exit(4)
	}
	fmt.Printf("All runs satisfy the claim %s.\n", s.claim.Name())
}

func triageText(counts map[string]int) string {
	var keys []string
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s=%d", k, counts[k])
	}
	return strings.Join(parts, " ")
}

// readTopology reads the node labels used in the views. Labels only decorate
// the pages, so a failure to read them names nodes by index.
func readTopology(dbPath string) *checker.Topology {
	t, err := checker.ReadTopology(dbPath)
	if err != nil {
		log.Printf("Warning: node labels unavailable, naming nodes by index: %v", err)
		return nil
	}
	return t
}

type history struct {
	label   string
	events  []*checker.EventRow
	rows    []engine.Row
	outcome checker.Outcome
}

func check(events []*checker.EventRow, label string, s settings) history {
	rows := checker.EngineRows(events)
	return history{label: label, events: events, rows: rows, outcome: checker.CheckHistory(rows, s.claim, s.timeout, true)}
}

// report prints the outcome and writes the page and, for a violation, the
// witness as text beside it.
func report(h history, o checker.Outcome, outputFile string, names checker.NodeNames, s settings) {
	switch o.Verdict {
	case engine.VerdictOK:
		fmt.Printf("%s: claim %s holds, triage %s\n", h.label, s.claim.Name(), o.Triage)
	case engine.VerdictIllegal:
		fmt.Printf("%s: claim %s VIOLATED, triage %s\n", h.label, s.claim.Name(), o.Triage)
	default:
		fmt.Printf("%s: claim %s unknown (%s), triage %s\n", h.label, s.claim.Name(), o.Reason, o.Triage)
	}

	base := strings.TrimSuffix(outputFile, ".html")
	events := checker.ViewEvents(h.events, h.rows)
	if o.Verdict == engine.VerdictIllegal {
		text, err := checker.ClaimText(h.label, s.claim, o, events)
		if err == nil {
			err = os.WriteFile(base+".claim.txt", []byte(text), 0o644)
		}
		if err != nil {
			log.Printf("Warning: failed to write the witness to %s.claim.txt: %v", base, err)
		} else {
			fmt.Printf("Witness written to %s.claim.txt\n", base)
		}
	}

	in, err := checker.ViewInput(h.label, s.claim, o, events, names)
	if err == nil {
		err = view.WritePath(outputFile, in)
	}
	if err != nil {
		log.Printf("Warning: failed to write visualization to %s: %v", outputFile, err)
	} else {
		fmt.Printf("Visualization written to %s\n", outputFile)
	}
}

func exitForVerdict(verdict string) {
	switch verdict {
	case engine.VerdictIllegal:
		os.Exit(2)
	case engine.VerdictUnknown:
		os.Exit(4)
	}
}
