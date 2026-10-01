package checker

import (
	"strings"
	"time"

	"github.com/benaepli/turnpike-porcupine/checker/engine"
)

// Outcome is the check of one history: the claim's verdict, the reason when
// it is undecided, the triage, and the claim's canonical witness, empty when
// there is none.
type Outcome struct {
	Verdict string
	Reason  string
	Triage  string
	Witness string
}

// EngineRows converts history rows for the engine. Every row is passed, and
// the engine classifies each by its own kind and action.
func EngineRows(events []*EventRow) []engine.Row {
	rows := make([]engine.Row, len(events))
	for i, e := range events {
		rows[i] = engine.RowFromEvent(e.UniqueID, e.ClientID, e.Kind, e.Action, e.Payload)
	}
	return rows
}

// SkippedOps counts the invocations and responses whose action is neither a
// client operation nor one of the system actions the simulator records, the
// count the simulator stores beside its verdicts.
func SkippedOps(events []*EventRow) int {
	n := 0
	for _, e := range events {
		if e.Kind != "Invocation" && e.Kind != "Response" {
			continue
		}
		if _, ok := engine.ActionKind(e.Action); ok {
			continue
		}
		switch {
		case strings.HasSuffix(e.Action, "System.Crash"),
			strings.HasSuffix(e.Action, "System.Recover"),
			strings.HasSuffix(e.Action, "Client.SimulateTimeout"),
			strings.HasSuffix(e.Action, "Client.Delete"):
			continue
		}
		n++
	}
	return n
}

// CheckHistory checks a history against a claim in stages, each under its
// own deadline of timeout (zero means none): linearizability first, then the
// claim when it is weaker, then the remaining ladder levels for the triage.
// A violation always carries its witness. A passing history carries one only
// when witness is set; without it, a linearizable history is not checked
// further.
func CheckHistory(rows []engine.Row, c engine.Claim, timeout time.Duration, witness bool) Outcome {
	h := engine.Prepare(rows, c.Model)
	return CheckPrepared(h, c, timeout, witness)
}

// CheckPrepared is CheckHistory on a prepared history.
func CheckPrepared(h *engine.History, c engine.Claim, timeout time.Duration, witness bool) Outcome {
	stage := func() engine.Interrupt {
		if timeout <= 0 {
			return engine.Interrupt{}
		}
		return engine.Interrupt{Deadline: time.Now().Add(timeout)}
	}
	if h.AdapterError() != "" {
		return Outcome{Verdict: engine.VerdictUnknown, Reason: "adapter_error: " + h.AdapterError(), Triage: engine.VerdictUnknown}
	}
	ladder := engine.Ladder(c.Model)
	lc := c.Linearizable()
	lin := h.CheckClaim(ladder[0], stage())

	var claim engine.ClaimResult
	switch {
	case lc == ladder[0].Linearizable():
		claim = lin
	case lin.Verdict == engine.VerdictOK && !witness:
		claim = engine.ClaimResult{Verdict: engine.VerdictOK}
	default:
		claim = h.CheckClaim(c, stage())
	}

	out := Outcome{Verdict: claim.Verdict, Reason: claim.Reason}
	if claim.Witness != nil && (witness || claim.Verdict == engine.VerdictIllegal) {
		out.Witness = claim.Witness.String()
	}
	out.Triage = triage(h, c, claim, lin, stage)
	return out
}

// triage walks the ladder strongest first. A level is decided by the
// linearizability stage, by the claim's own verdict where the claim implies
// or is implied by the level, or else by its own check.
func triage(h *engine.History, c engine.Claim, claim, lin engine.ClaimResult, stage func() engine.Interrupt) string {
	lc := c.Linearizable()
	for i, lvl := range engine.Ladder(c.Model) {
		ll := lvl.Linearizable()
		var verdict string
		switch {
		case i == 0:
			verdict = lin.Verdict
		case ll == lc:
			verdict = claim.Verdict
		case claim.Verdict == engine.VerdictOK && ll&^lc == 0:
			verdict = engine.VerdictOK
		case claim.Verdict == engine.VerdictIllegal && lc&^ll == 0:
			verdict = engine.VerdictIllegal
		default:
			verdict = h.CheckClaim(lvl, stage()).Verdict
		}
		switch verdict {
		case engine.VerdictOK:
			return lvl.Name()
		case engine.VerdictUnknown:
			return engine.VerdictUnknown
		}
	}
	return "none"
}
