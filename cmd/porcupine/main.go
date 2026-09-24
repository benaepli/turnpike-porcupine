package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/gocarina/gocsv"

	"github.com/benaepli/turnpike-porcupine/checker"
	"github.com/benaepli/turnpike-porcupine/stats"
)

func main() {
	inputFile := flag.String("input", "", "Path to the input file (CSV or DuckDB database) (required)")
	inputType := flag.String("type", "duckdb", "Input type: 'csv' or 'duckdb' (default: duckdb)")
	runID := flag.Int("run", -1, "Run ID to check (DuckDB only; -1 means all runs)")
	outputFile := flag.String("output", "", "Path for output HTML file (single run) or directory (all runs)")
	outputDir := flag.String("output-dir", "", "Output directory for HTML files (when processing all runs)")
	modelName := flag.String("model", "", "Model to check: kv|kv_rmw|queue (default: the model recorded in the deployments table; required for CSV)")
	timeoutMs := flag.Int("timeout", 0, "Per-run timeout in milliseconds (0 = no timeout)")
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
	}

	var model porcupine.Model
	switch *modelName {
	case "kv":
		model = checker.KVModel()
	case "kv_rmw":
		model = checker.KVRMWModel()
	case "queue":
		model = checker.QueueModel()
	default:
		log.Fatalf("unknown model %q (use kv|kv_rmw|queue)", *modelName)
	}

	if inputTypeNorm == "csv" {
		// CSV mode - original behavior
		if *outputFile == "" {
			log.Fatalln("Error: -output flag is required for CSV mode.")
		}
		processCSV(*inputFile, *outputFile, model, *timeoutMs)
	} else {
		// DuckDB mode
		if *runID == -1 {
			// Process all runs
			// Prefer -output-dir, fall back to -output
			outDir := *outputDir
			if outDir == "" {
				outDir = *outputFile
			}
			processAllRuns(*inputFile, outDir, *modelName, model, *timeoutMs, *recheckAll)
		} else {
			// Process single run
			if *outputFile == "" {
				log.Fatalln("Error: -output flag is required when checking a single run.")
			}
			processSingleRun(*inputFile, *runID, *outputFile, *modelName, model, *timeoutMs)
		}
	}
}

func processCSV(inputFile, outputFile string, model porcupine.Model, timeoutMs int) {
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

	ops, annotations := checker.BuildOperationsWithAnnotations(eventRows)
	exitForVerdict(checkAndVisualize(model, ops, annotations, outputFile, "CSV", timeoutMs))
}

func processSingleRun(dbPath string, runID int, outputFile, modelName string, model porcupine.Model, timeoutMs int) {
	cache, err := checker.LoadCheckCache(dbPath, modelName)
	if err != nil {
		log.Fatalf("failed to read check metadata: %v", err)
	}
	eventRows, err := checker.ReadEventsFromDuckDB(dbPath, runID)
	if err != nil {
		log.Fatalf("failed to read events from DuckDB: %v", err)
	}

	ops, annotations := checker.BuildOperationsWithNodeNames(eventRows, readTopology(dbPath).NamesForRun(runID))
	verdict := checkAndVisualize(model, ops, annotations, outputFile, fmt.Sprintf("Run %d", runID), timeoutMs)
	if cached, ok := cache.Runs[runID]; ok {
		verdict, err = checker.ReconcileVerdict(runID, verdict, &cached)
		if err != nil {
			log.Fatal(err)
		}
	}
	exitForVerdict(verdict)
}

func processAllRuns(dbPath, outputDir, modelName string, model porcupine.Model, timeoutMs int, recheckAll bool) {
	// Create output directory if specified and doesn't exist
	if outputDir != "" {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			log.Fatalf("failed to create output directory %s: %v", outputDir, err)
		}
	}

	violations, unknown, reused := 0, 0, 0
	runCount := 0
	var results []stats.RunResult
	topology := readTopology(dbPath)

	err := checker.ProcessRunsWithCache(dbPath, modelName, recheckAll, func(runID int, eventRows []*checker.EventRow, cached *checker.CachedVerdict, reuse bool) error {
		runCount++
		if reuse {
			reused++
			return nil
		}
		ops, annotations := checker.BuildOperationsWithNodeNames(eventRows, topology.NamesForRun(runID))

		// Generate output filename
		var outFile string
		if outputDir != "" {
			outFile = filepath.Join(outputDir, fmt.Sprintf("run_%d.html", runID))
		} else {
			outFile = fmt.Sprintf("run_%d.html", runID)
		}

		fmt.Printf("\n=== Checking Run %d ===\n", runID)
		start := time.Now()
		verdict := checkAndVisualize(model, ops, annotations, outFile, fmt.Sprintf("Run %d", runID), timeoutMs)
		var err error
		verdict, err = checker.ReconcileVerdict(runID, verdict, cached)
		if err != nil {
			return err
		}
		elapsed := time.Since(start)

		results = append(results, stats.RunResult{
			FileName:     fmt.Sprintf("run_%d", runID),
			ElapsedTime:  elapsed,
			Success:      verdict != porcupine.Unknown,
			Linearizable: verdict == porcupine.Ok,
		})

		if verdict == porcupine.Illegal {
			violations++
		}
		if verdict == porcupine.Unknown {
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

	// Print slow runs
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
			fmt.Printf("  %s: %v (linearizable: %v)\n", r.FileName, r.ElapsedTime.Round(time.Millisecond), r.Linearizable)
		}
	}

	fmt.Printf("\n=== Summary (%d runs) ===\n", runCount)
	fmt.Printf("%d reused passes (HTML omitted), %d violations, %d unknown\n", reused, violations, unknown)
	if violations > 0 {
		os.Exit(2)
	}
	if unknown > 0 {
		os.Exit(4)
	}
	fmt.Println("All runs are linearizable.")
}

// readTopology reads the node labels used in annotations. Labels only
// decorate the visualization, so a failure to read them names nodes by index.
func readTopology(dbPath string) *checker.Topology {
	t, err := checker.ReadTopology(dbPath)
	if err != nil {
		log.Printf("Warning: node labels unavailable, naming nodes by index: %v", err)
		return nil
	}
	return t
}

func checkAndVisualize(model porcupine.Model, ops []porcupine.Operation, annotations []porcupine.Annotation, outputFile, label string, timeoutMs int) porcupine.CheckResult {
	res, info := porcupine.CheckOperationsVerbose(model, ops, time.Duration(timeoutMs)*time.Millisecond)

	if res == porcupine.Ok {
		fmt.Printf("%s: Linearizable? true\n", label)
	} else if res == porcupine.Illegal {
		fmt.Printf("%s: Linearizable? false\n", label)
	} else {
		fmt.Printf("%s: Unknown (check failed or timed out)\n", label)
	}

	// Add system event annotations (Crash/Recover/Timeout) as overlays
	if len(annotations) > 0 {
		info.AddAnnotations(annotations)
	}

	if err := porcupine.VisualizePath(model, info, outputFile); err != nil {
		log.Printf("Warning: failed to write visualization to %s: %v", outputFile, err)
	} else {
		fmt.Printf("Visualization written to %s\n", outputFile)
	}

	return res
}

func exitForVerdict(verdict porcupine.CheckResult) {
	if verdict == porcupine.Illegal {
		os.Exit(2)
	}
	if verdict == porcupine.Unknown {
		os.Exit(4)
	}
}
