package engine

import (
	"testing"

	"github.com/anishathalye/porcupine"
)

// BenchmarkFixturesEngine checks every fixture without an interrupt: the
// claim and the ladder walk, witness included.
func BenchmarkFixturesEngine(b *testing.B) {
	fs := loadFixtures(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range fs {
			if fs[j].Interrupt == nil {
				Check(fs[j].rows(), fs[j].claim(b), Interrupt{})
			}
		}
	}
}

// BenchmarkFixturesLinearizableEngine and
// BenchmarkFixturesLinearizableUpstream check the fixtures whose claim is
// linearizable, the only claim upstream porcupine can check.
func BenchmarkFixturesLinearizableEngine(b *testing.B) {
	rows, models := linearizableFixtures(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range rows {
			Prepare(rows[j], models[j]).CheckClaim(ClaimWith(models[j], models[j].Declared()), Interrupt{})
		}
	}
}

func BenchmarkFixturesLinearizableUpstream(b *testing.B) {
	rows, models := linearizableFixtures(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range rows {
			porcupine.CheckOperations(upstreamModel(models[j]), upstreamOperations(rows[j]))
		}
	}
}

func linearizableFixtures(b *testing.B) ([][]Row, []Model) {
	var rows [][]Row
	var models []Model
	for _, f := range loadFixtures(b) {
		c := f.claim(b)
		if f.Interrupt == nil && c.Name() == "linearizable" && f.Expect.Verdict != VerdictUnknown {
			rows = append(rows, f.rows())
			models = append(models, c.Model)
		}
	}
	return rows, models
}
