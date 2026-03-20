package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/gocarina/gocsv"

	"github.com/benaepli/turnpike-porcupine/checker"
)

type RunResult struct {
	FileName     string
	ElapsedTime  time.Duration
	Success      bool
	Linearizable bool
}

type BenchmarkStats struct {
	TotalRuns           int
	SuccessfulRuns      int
	FailedRuns          int
	LinearizableRuns    int
	NonLinearizableRuns int
	TotalTime           time.Duration
	MeanTime            time.Duration
	MinTime             time.Duration
	MaxTime             time.Duration
	StdDev              time.Duration
	RunTimes            []time.Duration
}

func calculateStats(results []RunResult) BenchmarkStats {
	successfulResults := make([]RunResult, 0)
	linearizableCount := 0
	nonLinearizableCount := 0

	for _, r := range results {
		if r.Success {
			successfulResults = append(successfulResults, r)
			if r.Linearizable {
				linearizableCount++
			} else {
				nonLinearizableCount++
			}
		}
	}

	totalRuns := len(results)
	successfulRuns := len(successfulResults)
	failedRuns := totalRuns - successfulRuns

	if successfulRuns == 0 {
		return BenchmarkStats{
			TotalRuns:           totalRuns,
			SuccessfulRuns:      0,
			FailedRuns:          failedRuns,
			LinearizableRuns:    0,
			NonLinearizableRuns: 0,
			TotalTime:           0,
			MeanTime:            0,
			MinTime:             0,
			MaxTime:             0,
			StdDev:              0,
			RunTimes:            []time.Duration{},
		}
	}

	runTimes := make([]time.Duration, 0, successfulRuns)
	for _, r := range successfulResults {
		runTimes = append(runTimes, r.ElapsedTime)
	}

	var totalTime time.Duration
	for _, t := range runTimes {
		totalTime += t
	}

	meanTime := totalTime / time.Duration(successfulRuns)

	minTime := runTimes[0]
	maxTime := runTimes[0]
	for _, t := range runTimes {
		if t < minTime {
			minTime = t
		}
		if t > maxTime {
			maxTime = t
		}
	}

	// Calculate standard deviation
	var variance float64
	meanFloat := float64(meanTime.Nanoseconds())
	for _, t := range runTimes {
		diff := float64(t.Nanoseconds()) - meanFloat
		variance += diff * diff
	}
	if successfulRuns > 1 {
		variance /= float64(successfulRuns - 1)
	}
	stdDev := time.Duration(math.Sqrt(variance))

	return BenchmarkStats{
		TotalRuns:           totalRuns,
		SuccessfulRuns:      successfulRuns,
		FailedRuns:          failedRuns,
		LinearizableRuns:    linearizableCount,
		NonLinearizableRuns: nonLinearizableCount,
		TotalTime:           totalTime,
		MeanTime:            meanTime,
		MinTime:             minTime,
		MaxTime:             maxTime,
		StdDev:              stdDev,
		RunTimes:            runTimes,
	}
}

func printSummary(stats BenchmarkStats) {
	line := "============================================================"
	fmt.Printf("\n%s\n", line)
	fmt.Println("📊 BENCHMARK SUMMARY")
	fmt.Printf("%s\n", line)
	fmt.Printf("Total Runs:         %d\n", stats.TotalRuns)
	fmt.Printf("Successful Runs:    %d\n", stats.SuccessfulRuns)

	if stats.FailedRuns > 0 {
		fmt.Printf("Failed Runs:        %d\n", stats.FailedRuns)
	}

	if stats.SuccessfulRuns > 0 {
		fmt.Printf("Linearizable:       %d\n", stats.LinearizableRuns)
		fmt.Printf("Non-Linearizable:   %d\n", stats.NonLinearizableRuns)

		separator := "------------------------------------------------------------"
		fmt.Printf("\n%s\n", separator)
		fmt.Println("TIMING STATISTICS")
		fmt.Printf("%s\n", separator)
		fmt.Printf("Total Time:         %v\n", stats.TotalTime.Round(time.Millisecond))
		fmt.Printf("Mean Time:          %v\n", stats.MeanTime.Round(time.Millisecond))

		if stats.SuccessfulRuns > 1 {
			fmt.Printf("Min Time:           %v\n", stats.MinTime.Round(time.Millisecond))
			fmt.Printf("Max Time:           %v\n", stats.MaxTime.Round(time.Millisecond))
			fmt.Printf("Std. Deviation:     %v\n", stats.StdDev.Round(time.Millisecond))

			if stats.MeanTime > 0 {
				cv := (float64(stats.StdDev) / float64(stats.MeanTime)) * 100.0
				fmt.Printf("Coefficient of Variation: %.2f%%\n", cv)
			}
		}

		fmt.Printf("\n%s\n", separator)
	} else {
		fmt.Println("\n No successful runs to report.")
	}
	fmt.Printf("%s\n\n", line)
}

func processFile(filename string, model porcupine.Model) RunResult {
	fmt.Printf("\nProcessing %s... ", filepath.Base(filename))

	startTime := time.Now()

	f, err := os.Open(filename)
	if err != nil {
		fmt.Printf("FAILED (could not open file: %v)\n", err)
		return RunResult{
			FileName:     filepath.Base(filename),
			ElapsedTime:  time.Since(startTime),
			Success:      false,
			Linearizable: false,
		}
	}
	defer f.Close()

	var eventRows []*checker.EventRow
	if err := gocsv.UnmarshalFile(f, &eventRows); err != nil {
		fmt.Printf("FAILED (could not parse CSV: %v)\n", err)
		return RunResult{
			FileName:     filepath.Base(filename),
			ElapsedTime:  time.Since(startTime),
			Success:      false,
			Linearizable: false,
		}
	}

	ops := checker.BuildOperations(eventRows)
	res, _ := porcupine.CheckOperationsVerbose(model, ops, 0)

	elapsedTime := time.Since(startTime)

	success := res == porcupine.Ok || res == porcupine.Illegal
	linearizable := res == porcupine.Ok

	if success {
		if linearizable {
			fmt.Printf("✓ Linearizable (%v)\n", elapsedTime.Round(time.Millisecond))
		} else {
			fmt.Printf("✗ Not Linearizable (%v)\n", elapsedTime.Round(time.Millisecond))
		}
	} else {
		fmt.Printf("FAILED (check error) (%v)\n", elapsedTime.Round(time.Millisecond))
	}

	return RunResult{
		FileName:     filepath.Base(filename),
		ElapsedTime:  elapsedTime,
		Success:      success,
		Linearizable: linearizable,
	}
}

func main() {
	inputDir := flag.String("dir", "", "Path to directory containing CSV files (required)")
	modelName := flag.String("model", "", "Model to check (e.g., 'kv', 'queue') (required)")
	flag.Parse()

	if *inputDir == "" || *modelName == "" {
		flag.Usage()
		log.Fatalln("Error: -dir and -model flags are required.")
	}

	// Get model
	var model porcupine.Model
	switch *modelName {
	case "kv":
		model = checker.KVModel()
	case "queue":
		model = checker.QueueModel()
	default:
		log.Fatalf("unknown model %q (use kv|queue)", *modelName)
	}

	// Find all CSV files in directory
	pattern := filepath.Join(*inputDir, "*.csv")
	files, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("failed to read directory: %v", err)
	}

	if len(files) == 0 {
		log.Fatalf("no CSV files found in directory: %s", *inputDir)
	}

	// Sort files for consistent ordering
	sort.Strings(files)

	// Print header
	line := "============================================================"
	fmt.Printf("%s\n", line)
	fmt.Println("STARTING BENCHMARK")
	fmt.Printf("%s\n", line)
	fmt.Printf("Directory:          %s\n", *inputDir)
	fmt.Printf("Model:              %s\n", *modelName)
	fmt.Printf("Number of Files:    %d\n", len(files))
	fmt.Printf("%s\n", line)

	// Process all files
	results := make([]RunResult, 0, len(files))
	for _, file := range files {
		result := processFile(file, model)
		results = append(results, result)
	}

	// Calculate and print statistics
	stats := calculateStats(results)
	printSummary(stats)
}
