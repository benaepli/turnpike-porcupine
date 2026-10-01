// Fast batch checker: no HTML, machine-readable JSON output, per-stage
// timeouts, and Unknown-vs-Illegal separation. Every history is checked
// against the specification's consistency claim.
//
// JSON goes to stdout (and optionally to -json <path>); human-readable
// progress goes to stderr.
//
// Exit codes:
//
//	0 - every history satisfies the claim
//	1 - usage / IO / query error
//	2 - at least one history violates the claim
//	3 - no runs found in the input
//	4 - no violations, but at least one run was Unknown (timeout / check failure)
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/benaepli/turnpike-porcupine/checker"
	"github.com/benaepli/turnpike-porcupine/checker/engine"
	"github.com/benaepli/turnpike-porcupine/checker/view"
)

// Result is the machine-readable summary consumed by the research harness.
type Result struct {
	ReusedRuns       int    `json:"reused_runs"`
	NewlyCheckedRuns int    `json:"newly_checked_runs"`
	Input            string `json:"input"`
	Model            string `json:"model"`
	// Claim is the claim's marks, and ClaimName its display name.
	Claim     map[string]string `json:"claim"`
	ClaimName string            `json:"claim_name"`
	TotalRuns int               `json:"total_runs"`
	Ok        int               `json:"ok"`
	// Violations counts histories that violate the claim.
	Violations int `json:"violations"`
	Unknown    int `json:"unknown"`
	// Triage counts histories by the strongest consistency level each
	// satisfies: linearizable, ordered_sequential, sequential, none, or
	// unknown.
	Triage          map[string]int `json:"triage"`
	SkippedOps      int            `json:"skipped_ops"`
	ViolatingRunIDs []int          `json:"violating_run_ids"`
	UnknownRunIDs   []int          `json:"unknown_run_ids"`
	WallMs          int64          `json:"wall_ms"`
	// Position of the first violating run in run_id order (1-based) and its
	// id, so a consumer can measure time to the first violation from the
	// runs table. Absent when nothing violated.
	FirstViolationOrdinal int  `json:"first_violation_ordinal,omitempty"`
	FirstViolationRunID   *int `json:"first_violation_run_id,omitempty"`
	// A fingerprint of each violating run's witness, capped, so distinct
	// violations can be told apart from repeats of one.
	ViolationSignatures []Signature `json:"violation_signatures,omitempty"`
}

// Signature identifies the shape of one violation.
type Signature struct {
	RunID     int    `json:"run_id"`
	Ordinal   int    `json:"ordinal"`
	Signature string `json:"signature"`
}

// The signature list is bounded so a corpus that violates everywhere does
// not turn the summary into a dump.
const maxSignatures = 200

func main() {
	inputPath := flag.String("input", "", "Path to DuckDB file or Parquet output directory (required)")
	modelName := flag.String("model", "", "Model to check: kv|kv_rmw (default: the model recorded in the deployments table)")
	claimFlag := flag.String("claim", "", `Claim to check, as its JSON object (default: the claim recorded in the deployments table, else every operation linearizable)`)
	timeoutMs := flag.Int("timeout", 10000, "Per-stage check timeout in milliseconds (0 = no timeout)")
	jsonPath := flag.String("json", "", "Also write the JSON result to this file (optional)")
	recheckAll := flag.Bool("recheck-all", false, "Recheck every history independently, bypassing cached verdicts")
	flag.Parse()

	// `porcupine_batch <dir>` positional form.
	if *inputPath == "" && flag.NArg() == 1 {
		*inputPath = flag.Arg(0)
	}
	if *inputPath == "" || flag.NArg() > 1 {
		flag.Usage()
		log.Fatalln("Error: -input is required.")
	}
	fail := func(err error) {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	resolved, warning, err := checker.ResolveModel(*modelName, *inputPath)
	if err != nil {
		fail(err)
	}
	if warning != "" {
		fmt.Fprintf(os.Stderr, "warning: %s\n", warning)
	}
	if warning := checker.SymbolicWarning(*inputPath); warning != "" {
		fmt.Fprintf(os.Stderr, "warning: %s\n", warning)
	}
	model, err := engine.ParseModel(resolved)
	if err != nil {
		fail(fmt.Errorf("unknown model %q (use kv|kv_rmw)", resolved))
	}
	claim, warning, err := checker.ResolveClaim(*claimFlag, model, *inputPath)
	if err != nil {
		fail(err)
	}
	if warning != "" {
		fmt.Fprintf(os.Stderr, "warning: %s\n", warning)
	}
	timeout := time.Duration(*timeoutMs) * time.Millisecond

	res := Result{
		Input:           *inputPath,
		Model:           model.String(),
		Claim:           checker.ClaimMarks(claim),
		ClaimName:       claim.Name(),
		Triage:          map[string]int{},
		ViolatingRunIDs: []int{},
		UnknownRunIDs:   []int{},
	}
	start := time.Now()

	err = checker.ProcessRunsWithCache(*inputPath, model.String(), claim.JSON(), *recheckAll, func(runID int, events []*checker.EventRow, cached *checker.CachedVerdict, reuse bool) error {
		res.TotalRuns++
		if reuse {
			res.ReusedRuns++
			res.Ok++
			res.Triage[cached.Triage]++
			res.SkippedOps += cached.SkippedOps
			return nil
		}
		res.NewlyCheckedRuns++
		res.SkippedOps += checker.SkippedOps(events)
		rows := checker.EngineRows(events)
		outcome, err := checker.ReconcileVerdict(runID, checker.CheckHistory(rows, claim, timeout, false), cached)
		if err != nil {
			return err
		}
		res.Triage[outcome.Triage]++
		switch outcome.Verdict {
		case engine.VerdictOK:
			res.Ok++
		case engine.VerdictIllegal:
			res.Violations++
			res.ViolatingRunIDs = append(res.ViolatingRunIDs, runID)
			if res.FirstViolationRunID == nil {
				id := runID
				res.FirstViolationRunID = &id
				res.FirstViolationOrdinal = res.TotalRuns
			}
			if len(res.ViolationSignatures) < maxSignatures {
				if w, err := view.ParseWitness(outcome.Witness); err == nil && w != nil {
					res.ViolationSignatures = append(res.ViolationSignatures, Signature{
						RunID: runID, Ordinal: res.TotalRuns,
						Signature: checker.WitnessSignature(model.String(), w, checker.ViewEvents(events, rows)),
					})
				}
			}
			if res.Violations <= 20 {
				fmt.Fprintf(os.Stderr, "run %d: claim %s VIOLATED, triage %s\n", runID, claim.Name(), outcome.Triage)
			}
		default:
			res.Unknown++
			res.UnknownRunIDs = append(res.UnknownRunIDs, runID)
			fmt.Fprintf(os.Stderr, "run %d: UNKNOWN (%s)\n", runID, outcome.Reason)
		}
		return nil
	})
	if err != nil {
		fail(err)
	}
	res.WallMs = time.Since(start).Milliseconds()

	fmt.Fprintf(os.Stderr, "\n=== %s (model=%s claim=%s) ===\n", *inputPath, res.Model, res.ClaimName)
	fmt.Fprintf(os.Stderr, "total=%d ok=%d violations=%d unknown=%d skipped_ops=%d wall_ms=%d triage=%v\n",
		res.TotalRuns, res.Ok, res.Violations, res.Unknown, res.SkippedOps, res.WallMs, res.Triage)

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(&res); err != nil {
		log.Fatalf("failed to encode JSON: %v", err)
	}
	if *jsonPath != "" {
		f, err := os.Create(*jsonPath)
		if err != nil {
			log.Fatalf("failed to create %s: %v", *jsonPath, err)
		}
		fenc := json.NewEncoder(f)
		fenc.SetIndent("", "  ")
		if err := fenc.Encode(&res); err != nil {
			log.Fatalf("failed to write %s: %v", *jsonPath, err)
		}
		_ = f.Close()
	}

	switch {
	case res.TotalRuns == 0:
		os.Exit(3)
	case res.Violations > 0:
		os.Exit(2)
	case res.Unknown > 0:
		os.Exit(4)
	}
}
