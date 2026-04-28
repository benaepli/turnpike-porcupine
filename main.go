package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/anishathalye/porcupine"
	"github.com/gocarina/gocsv"

	"github.com/benaepli/turnpike-porcupine/checker"
)

func main() {
	inputFile := flag.String("input", "", "Path to the input file (CSV or DuckDB database) (required)")
	inputType := flag.String("type", "duckdb", "Input type: 'csv' or 'duckdb' (default: duckdb)")
	runID := flag.Int("run", -1, "Run ID to check (DuckDB only; -1 means all runs)")
	outputFile := flag.String("output", "", "Path for output HTML file (single run) or directory (all runs)")
	outputDir := flag.String("output-dir", "", "Output directory for HTML files (when processing all runs)")
	modelName := flag.String("model", "", "Model to check (e.g., 'kv', 'kv_rmw', 'queue') (required)")
	flag.Parse()

	// Validate required flags
	if *inputFile == "" || *modelName == "" {
		flag.Usage()
		log.Fatalln("Error: -input and -model flags are required.")
	}

	inputTypeNorm := strings.ToLower(*inputType)
	if inputTypeNorm != "csv" && inputTypeNorm != "duckdb" {
		log.Fatalf("invalid input type %q (use csv|duckdb)", *inputType)
	}

	// Get the model
	var model porcupine.Model
	// Note: the kv and kv_rmw models are NOT interchangeable. kv_rmw expects
	// Read responses to be a VList of VTuple(VOption(VInt), VInt) — the per-key
	// log of (prev_uid, uid) entries used by Gryff-style protocols. Specs
	// targeting kv_rmw must store list<(int?, int)> and return that shape from
	// ClientInterface.Read; specs storing list<int> belong with -model kv.
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
		processCSV(*inputFile, *outputFile, model)
	} else {
		// DuckDB mode
		if *runID == -1 {
			// Process all runs
			// Prefer -output-dir, fall back to -output
			outDir := *outputDir
			if outDir == "" {
				outDir = *outputFile
			}
			processAllRuns(*inputFile, outDir, model)
		} else {
			// Process single run
			if *outputFile == "" {
				log.Fatalln("Error: -output flag is required when checking a single run.")
			}
			processSingleRun(*inputFile, *runID, *outputFile, model)
		}
	}
}

func processCSV(inputFile, outputFile string, model porcupine.Model) {
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
	checkAndVisualize(model, ops, annotations, outputFile, "CSV")
}

func processSingleRun(dbPath string, runID int, outputFile string, model porcupine.Model) {
	eventRows, err := checker.ReadEventsFromDuckDB(dbPath, runID)
	if err != nil {
		log.Fatalf("failed to read events from DuckDB: %v", err)
	}

	ops, annotations := checker.BuildOperationsWithAnnotations(eventRows)
	checkAndVisualize(model, ops, annotations, outputFile, fmt.Sprintf("Run %d", runID))
}

func processAllRuns(dbPath, outputDir string, model porcupine.Model) {
	// Create output directory if specified and doesn't exist
	if outputDir != "" {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			log.Fatalf("failed to create output directory %s: %v", outputDir, err)
		}
	}

	allLinearizable := true
	runCount := 0

	err := checker.ProcessAllRunsFromDuckDB(dbPath, func(runID int, eventRows []*checker.EventRow) error {
		runCount++
		ops, annotations := checker.BuildOperationsWithAnnotations(eventRows)

		// Generate output filename
		var outFile string
		if outputDir != "" {
			outFile = filepath.Join(outputDir, fmt.Sprintf("run_%d.html", runID))
		} else {
			outFile = fmt.Sprintf("run_%d.html", runID)
		}

		fmt.Printf("\n=== Checking Run %d ===\n", runID)
		if !checkAndVisualize(model, ops, annotations, outFile, fmt.Sprintf("Run %d", runID)) {
			allLinearizable = false
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

	fmt.Printf("\n=== Summary (%d runs) ===\n", runCount)
	if allLinearizable {
		fmt.Println("All runs are linearizable.")
	} else {
		fmt.Println("Some runs are NOT linearizable.")
		os.Exit(2)
	}
}

func checkAndVisualize(model porcupine.Model, ops []porcupine.Operation, annotations []porcupine.Annotation, outputFile, label string) bool {
	res, info := porcupine.CheckOperationsVerbose(model, ops, 0)

	if res == porcupine.Ok {
		fmt.Printf("%s: Linearizable? true\n", label)
	} else if res == porcupine.Illegal {
		fmt.Printf("%s: Linearizable? false\n", label)
	} else {
		fmt.Printf("%s: Linearizable? Unknown (Check failed)\n", label)
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

	if res != porcupine.Ok {
		log.Printf("%s: History is NOT linearizable.\n", label)
		return false
	}
	return true
}
