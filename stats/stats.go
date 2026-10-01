package stats

import (
	"fmt"
	"math"
	"time"
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

func CalculateStats(results []RunResult) BenchmarkStats {
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

func PrintSummary(st BenchmarkStats) {
	line := "============================================================"
	fmt.Printf("\n%s\n", line)
	fmt.Println("BENCHMARK SUMMARY")
	fmt.Printf("%s\n", line)
	fmt.Printf("Total Runs:         %d\n", st.TotalRuns)
	fmt.Printf("Successful Runs:    %d\n", st.SuccessfulRuns)

	if st.FailedRuns > 0 {
		fmt.Printf("Failed Runs:        %d\n", st.FailedRuns)
	}

	if st.SuccessfulRuns > 0 {
		fmt.Printf("Claim holds:        %d\n", st.LinearizableRuns)
		fmt.Printf("Claim violated:     %d\n", st.NonLinearizableRuns)

		separator := "------------------------------------------------------------"
		fmt.Printf("\n%s\n", separator)
		fmt.Println("TIMING STATISTICS")
		fmt.Printf("%s\n", separator)
		fmt.Printf("Total Time:         %v\n", st.TotalTime.Round(time.Millisecond))
		fmt.Printf("Mean Time:          %v\n", st.MeanTime.Round(time.Millisecond))

		if st.SuccessfulRuns > 1 {
			fmt.Printf("Min Time:           %v\n", st.MinTime.Round(time.Millisecond))
			fmt.Printf("Max Time:           %v\n", st.MaxTime.Round(time.Millisecond))
			fmt.Printf("Std. Deviation:     %v\n", st.StdDev.Round(time.Millisecond))

			if st.MeanTime > 0 {
				cv := (float64(st.StdDev) / float64(st.MeanTime)) * 100.0
				fmt.Printf("Coefficient of Variation: %.2f%%\n", cv)
			}
		}

		fmt.Printf("\n%s\n", separator)
	} else {
		fmt.Println("\nNo successful runs to report.")
	}
	fmt.Printf("%s\n\n", line)
}
