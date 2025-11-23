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

	"github.com/benaepli/jennlang-porcupine/checker"
)

func main() {
	inputFile := flag.String("input", "", "Path to the input file (CSV or SQLite database) (required)")
	inputType := flag.String("type", "sqlite", "Input type: 'csv' or 'sqlite' (default: sqlite)")
	runID := flag.Int("run", -1, "Run ID to check (SQLite only; -1 means all runs)")
	outputFile := flag.String("output", "", "Path for the output visualization HTML file (required for single run)")
	modelName := flag.String("model", "", "Model to check (e.g., 'kv', 'queue') (required)")
	flag.Parse()

	// Validate required flags
	if *inputFile == "" || *modelName == "" {
		flag.Usage()
		log.Fatalln("Error: -input and -model flags are required.")
	}

	inputTypeNorm := strings.ToLower(*inputType)
	if inputTypeNorm != "csv" && inputTypeNorm != "sqlite" {
		log.Fatalf("invalid input type %q (use csv|sqlite)", *inputType)
	}

	// Get the model
	var model porcupine.Model
	switch *modelName {
	case "kv":
		model = checker.KVModel()
	case "queue":
		model = checker.QueueModel()
	default:
		log.Fatalf("unknown model %q (use kv|queue)", *modelName)
	}

	if inputTypeNorm == "csv" {
		// CSV mode - original behavior
		if *outputFile == "" {
			log.Fatalln("Error: -output flag is required for CSV mode.")
		}
		processCSV(*inputFile, *outputFile, model)
	} else {
		// SQLite mode
		if *runID == -1 {
			// Process all runs
			processAllRuns(*inputFile, *outputFile, model)
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

	ops := checker.BuildOperations(eventRows)
	checkAndVisualize(model, ops, outputFile, "CSV")
}

func processSingleRun(dbPath string, runID int, outputFile string, model porcupine.Model) {
	eventRows, err := checker.ReadEventsFromSQLite(dbPath, runID)
	if err != nil {
		log.Fatalf("failed to read events from SQLite: %v", err)
	}

	ops := checker.BuildOperations(eventRows)
	checkAndVisualize(model, ops, outputFile, fmt.Sprintf("Run %d", runID))
}

func processAllRuns(dbPath, outputDir string, model porcupine.Model) {
	runIDs, err := checker.ListRunIDs(dbPath)
	if err != nil {
		log.Fatalf("failed to list run IDs: %v", err)
	}

	if len(runIDs) == 0 {
		log.Println("No runs found in database.")
		return
	}

	fmt.Printf("Found %d run(s) in database. Checking all...\n", len(runIDs))

	allLinearizable := true
	for _, runID := range runIDs {
		eventRows, err := checker.ReadEventsFromSQLite(dbPath, runID)
		if err != nil {
			log.Printf("Warning: failed to read events for run %d: %v", runID, err)
			continue
		}

		ops := checker.BuildOperations(eventRows)

		// Generate output filename
		var outFile string
		if outputDir != "" {
			outFile = filepath.Join(outputDir, fmt.Sprintf("run_%d.html", runID))
		} else {
			outFile = fmt.Sprintf("run_%d.html", runID)
		}

		fmt.Printf("\n=== Checking Run %d ===\n", runID)
		if !checkAndVisualize(model, ops, outFile, fmt.Sprintf("Run %d", runID)) {
			allLinearizable = false
		}
	}

	fmt.Println("\n=== Summary ===")
	if allLinearizable {
		fmt.Println("All runs are linearizable.")
	} else {
		fmt.Println("Some runs are NOT linearizable.")
		os.Exit(2)
	}
}

func checkAndVisualize(model porcupine.Model, ops []porcupine.Operation, outputFile, label string) bool {
	res, info := porcupine.CheckOperationsVerbose(model, ops, 0)

	if res == porcupine.Ok {
		fmt.Printf("%s: Linearizable? true\n", label)
	} else if res == porcupine.Illegal {
		fmt.Printf("%s: Linearizable? false\n", label)
	} else {
		fmt.Printf("%s: Linearizable? Unknown (Check failed)\n", label)
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
