// Package view draws a client history and a claim witness as one
// self-contained HTML page: inline CSS, script and SVG, the data as an
// embedded JSON block, and no network access or external asset, so the page
// opens from a corpus directory and can be attached to a finding.
package view

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// Event is one history row, in the field names of the claim fixtures. Key
// and UID are nil where the row carries none; Value is the observed list of
// a read response or the prior list of an RMW response. Node names the node
// of a Crash or Recover row.
type Event struct {
	Kind       string  `json:"kind"`
	ID         int64   `json:"id"`
	Client     int64   `json:"client"`
	Action     string  `json:"action"`
	Key        *string `json:"key,omitempty"`
	UID        *int64  `json:"uid,omitempty"`
	Value      []int64 `json:"value,omitempty"`
	Node       int64   `json:"node"`
	Step       int64   `json:"step"`
	GlobalTime int64   `json:"global_time"`
}

// Witness is the structured claim witness, in the field names of its
// canonical serialization.
type Witness struct {
	Level   string        `json:"level"`
	Kind    string        `json:"kind"`
	Key     *string       `json:"key"`
	Order   []int64       `json:"order"`
	Cycle   []Edge        `json:"cycle"`
	Blocked []Blocked     `json:"blocked"`
	Value   *ValueFailure `json:"value"`
}

// Edge is one edge of a cycle witness.
type Edge struct {
	From     int64  `json:"from"`
	To       int64  `json:"to"`
	Relation string `json:"relation"`
}

// Blocked is one session head at the deepest visit of a frontier witness.
// After is set for reason "precedence"; State and Observed for "value".
type Blocked struct {
	Session  int64   `json:"session"`
	Op       int64   `json:"op"`
	Reason   string  `json:"reason"`
	After    []After `json:"after,omitempty"`
	State    []int64 `json:"state,omitempty"`
	Observed []int64 `json:"observed,omitempty"`
}

// After is one unplaced predecessor of a blocked head.
type After struct {
	Op       int64  `json:"op"`
	Relation string `json:"relation"`
}

// ValueFailure is the failed value check of a value witness.
type ValueFailure struct {
	Op     int64   `json:"op"`
	Reason string  `json:"reason"`
	UIDs   []int64 `json:"uids"`
	Other  *int64  `json:"other"`
}

// Input is everything one page shows.
type Input struct {
	// Title names the page, for example the run.
	Title string
	// Model is "kv" or "kv_rmw".
	Model string
	// Claim maps "Write", "Read" and "RMW" to "linearizable" or "sequential".
	Claim   map[string]string
	Verdict string
	Reason  string
	Triage  string
	Events  []Event
	// Witness is nil when the verdict is undecided.
	Witness *Witness
	// NodeLabel names a node for the system lanes; nil, or a false second
	// result, names it by index.
	NodeLabel func(node int64) (string, bool)
}

// ParseWitness reads a witness in its canonical serialization; "null" and
// the empty string give nil.
func ParseWitness(s string) (*Witness, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "null" {
		return nil, nil
	}
	var w Witness
	if err := json.Unmarshal([]byte(s), &w); err != nil {
		return nil, fmt.Errorf("parse witness: %w", err)
	}
	return &w, nil
}

//go:embed view.html.tmpl
var pageSource string

var pageTemplate = template.Must(template.New("view").Parse(pageSource))

// WritePath renders the page to a file at path.
func WritePath(path string, in Input) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if err := Render(f, in); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// Render writes the page for in to w.
func Render(w io.Writer, in Input) error {
	p, err := build(in)
	if err != nil {
		return err
	}
	return pageTemplate.Execute(w, p)
}

// Geometry, in CSS pixels.
const (
	padLeft    = 12.0
	padRight   = 28.0
	axisHeight = 30.0
	nodeRow    = 30.0
	laneGap    = 8.0
	sessionRow = 76.0
	barHeight  = 28.0
	badgeSpace = 22.0
	labelWidth = 150.0
	minColumn  = 26.0
	maxLabel   = 180.0
	minBar     = 10.0
)

var relationOrder = []string{"session", "real_time", "ww", "wr", "rw"}

var relationText = map[string]string{
	"session":   "same session, earlier",
	"real_time": "returned before the other was invoked (both kinds linearizable)",
	"ww":        "write order fixed by the observed lists",
	"wr":        "the read observed the write",
	"rw":        "the read did not observe the write",
}

var valueReasonText = map[string]string{
	"phantom_uid":  "its list holds a uid no write of the key carries",
	"repeated_uid": "its list holds a uid more than once",
	"not_prefix":   "its list and the longest list observed before it are not prefixes of one another",
	"same_prior":   "it returned the same prior list as an earlier RMW of the key",
}

type op struct {
	ID       int64
	Client   int64
	Kind     string
	Action   string
	Key      string
	HasKey   bool
	UID      int64
	HasUID   bool
	Value    []int64
	HasValue bool
	Call     int
	Ret      int
	Pending  bool
	Dropped  bool
	Sub      int
	Seq      int
	State    []int64
	HasState bool
}

type sysEvent struct {
	Index int
	Kind  string
	Node  int64
}

type page struct {
	Title       string
	Model       string
	ClaimName   string
	Marks       []markView
	Verdict     string
	Reason      string
	Triage      string
	HasWitness  bool
	Level       string
	WitnessKind string
	Key         string
	HasKey      bool
	Sessions    int
	OpCount     int
	Nodes       int

	Width      string
	Height     string
	LabelWidth string
	HasGT      bool
	Lanes      []laneView
	StepTicks  []tickView
	GTTicks    []tickView
	Bars       []barView
	Arrows     []arrowView
	SysMarks   []sysView
	Downtimes  []downView
	ValueLink  *arrowView
	Relations  []relView
	AnyLin     bool
	AnySeq     bool
	AnyPending bool

	Order     []orderItem
	OrderNote string
	CycleList []edgeItem
	Heads     []headView
	Value     *valueView
	Rows      []rowView
	Warnings  []string
	Data      template.JS
}

type markView struct{ Kind, Mark string }

type laneView struct {
	Label  string
	Sub    string
	Y      string
	H      string
	TextY  string
	SubY   string
	Node   bool
	Stripe bool
}

type tickView struct {
	X     string
	Label string
}

type barView struct {
	ID        int64
	Class     string
	Tooltip   string
	Rect      template.HTMLAttr
	Y         string
	Outline   template.HTMLAttr
	Lin       bool
	LinMark   template.HTMLAttr
	LabelBox  template.HTMLAttr
	LabelText string
	Seq       int
	SeqAt     template.HTMLAttr
	Blocked   bool
	BlockAt   template.HTMLAttr
	BlockY    string
	Callout   string
	CalloutAt template.HTMLAttr
	CalloutY  string
	ValueRole string
}

type arrowView struct {
	From     int64
	To       int64
	Relation string
	Class    string
	Path     template.HTMLAttr
	LabelAt  template.HTMLAttr
	Label    string
	LabelW   string
	LabelX   string
}

type sysView struct {
	Class   string
	Tooltip string
	At      template.HTMLAttr
	Line    template.HTMLAttr
	Y       string
	Top     string
	Bottom  string
}

type downView struct {
	Rect template.HTMLAttr
	Y    string
	H    string
}

type relView struct {
	Name string
	Text string
}

type orderItem struct {
	Pos   int
	ID    int64
	Desc  string
	State string
}

type edgeItem struct {
	From     int64
	FromDesc string
	To       int64
	ToDesc   string
	Relation string
}

type headView struct {
	Session  int64
	ID       int64
	Desc     string
	Reason   string
	After    []edgeItem
	Key      string
	State    string
	Observed string
}

type valueView struct {
	ID         int64
	Desc       string
	Reason     string
	ReasonText string
	UIDs       string
	List       string
	ListLabel  string
	HasOther   bool
	Other      int64
	OtherDesc  string
	OtherList  string
	OtherLabel string
}

type rowView struct {
	ID       int64
	Session  int64
	Kind     string
	Key      string
	UID      string
	Result   string
	Call     string
	Return   string
	CallGT   string
	ReturnGT string
	Mark     string
	Seq      string
	Class    string
}

func kindOf(action string) string {
	switch {
	case strings.HasSuffix(action, "Client.Write"):
		return "W"
	case strings.HasSuffix(action, "Client.Read"):
		return "R"
	case strings.HasSuffix(action, "Client.RMW"):
		return "M"
	}
	return ""
}

var kindName = map[string]string{"W": "Write", "R": "Read", "M": "RMW"}
var kindVerb = map[string]string{"W": "PUT", "R": "GET", "M": "RMW"}

// claimName is the display name of a claim's marks under the model.
func claimName(model string, claim map[string]string) string {
	declared := []string{"Write", "Read"}
	if model == "kv_rmw" {
		declared = append(declared, "RMW")
	}
	lin := 0
	readLin := false
	for _, k := range declared {
		if claim[k] == "linearizable" {
			lin++
			if k == "Read" {
				readLin = true
			}
		}
	}
	switch {
	case lin == len(declared):
		return "linearizable"
	case lin == 0:
		return "sequential"
	case !readLin && lin == len(declared)-1:
		return "ordered_sequential"
	}
	return "mixed"
}

// keyText shows a key as written when every rune is printable, and quoted
// with escapes otherwise.
func keyText(k string) string {
	for _, r := range k {
		if !unicode.IsPrint(r) {
			return strconv.QuoteToGraphic(k)
		}
	}
	if k == "" {
		return `""`
	}
	return k
}

func listText(l []int64) string {
	parts := make([]string, len(l))
	for i, v := range l {
		parts[i] = strconv.FormatInt(v, 10)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func num(f float64) string {
	return strconv.FormatFloat(math.Round(f*10)/10, 'f', -1, 64)
}

func (o *op) label() string {
	verb := kindVerb[o.Kind]
	if verb == "" {
		verb = o.Action
	}
	s := verb
	if o.HasKey {
		s += " " + keyText(o.Key)
	}
	if o.HasUID {
		s += " " + strconv.FormatInt(o.UID, 10)
	}
	if o.HasValue {
		s += " \u2192 " + listText(o.Value)
	}
	return s
}

// labelPixels estimates the bar width that shows the whole label, with
// room for the witness position badge.
func labelPixels(o *op) float64 {
	text := fmt.Sprintf("op %d %s", o.ID, o.label())
	return badgeSpace + 14 + 6.8*float64(len([]rune(text)))
}

func (o *op) desc() string {
	return fmt.Sprintf("op %d %s", o.ID, o.label())
}

// pos holds one coordinate in both axes; G is used only when the page
// offers the global time axis.
type pos struct{ S, G float64 }

type geom struct {
	hasGT bool
}

// attrs writes each named coordinate, with its global time value in a
// data-g- attribute the page script swaps in.
func (g geom) attrs(pairs ...interface{}) template.HTMLAttr {
	var b strings.Builder
	for i := 0; i+1 < len(pairs); i += 2 {
		name := pairs[i].(string)
		switch v := pairs[i+1].(type) {
		case pos:
			fmt.Fprintf(&b, ` %s="%s"`, name, num(v.S))
			if g.hasGT {
				fmt.Fprintf(&b, ` data-g-%s="%s"`, name, num(v.G))
			}
		case [2]string:
			fmt.Fprintf(&b, ` %s="%s"`, name, v[0])
			if g.hasGT {
				fmt.Fprintf(&b, ` data-g-%s="%s"`, name, v[1])
			}
		}
	}
	return template.HTMLAttr(strings.TrimSpace(b.String()))
}

func translate(x, y pos) [2]string {
	return [2]string{"translate(" + num(x.S) + " " + num(y.S) + ")", "translate(" + num(x.G) + " " + num(y.G) + ")"}
}

func build(in Input) (*page, error) {
	p := &page{
		Title:     in.Title,
		Model:     in.Model,
		Verdict:   in.Verdict,
		Reason:    in.Reason,
		Triage:    in.Triage,
		ClaimName: claimName(in.Model, in.Claim),
	}
	if p.Title == "" {
		p.Title = "History"
	}
	declared := []string{"Write", "Read"}
	if in.Model == "kv_rmw" {
		declared = append(declared, "RMW")
	}
	for _, k := range declared {
		m := in.Claim[k]
		if m == "" {
			m = "unmarked"
		}
		p.Marks = append(p.Marks, markView{Kind: k, Mark: m})
	}
	warn := func(format string, args ...interface{}) {
		p.Warnings = append(p.Warnings, fmt.Sprintf(format, args...))
	}

	ops := map[int64]*op{}
	var opList []*op
	var sys []sysEvent
	for i, e := range in.Events {
		switch e.Kind {
		case "Invocation":
			if _, dup := ops[e.ID]; dup {
				warn("operation %d is invoked twice; the second invocation is not drawn", e.ID)
				continue
			}
			o := &op{ID: e.ID, Client: e.Client, Kind: kindOf(e.Action), Action: e.Action, Call: i, Ret: -1}
			if e.Key != nil {
				o.Key, o.HasKey = *e.Key, true
			}
			if e.UID != nil {
				o.UID, o.HasUID = *e.UID, true
			}
			if o.Kind == "" {
				warn("operation %d has action %q, which is not a client operation", e.ID, e.Action)
			}
			ops[e.ID] = o
			opList = append(opList, o)
		case "Response":
			o, ok := ops[e.ID]
			if !ok || o.Ret >= 0 {
				warn("response at row %d has no open invocation of operation %d", i+1, e.ID)
				continue
			}
			o.Ret = i
			if e.Value != nil || o.Kind == "R" || o.Kind == "M" {
				o.Value, o.HasValue = e.Value, true
			}
		case "Crash", "Recover":
			sys = append(sys, sysEvent{Index: i, Kind: e.Kind, Node: e.Node})
		default:
			warn("row %d has kind %q, which is not drawn", i+1, e.Kind)
		}
	}
	for _, o := range opList {
		if o.Ret < 0 {
			o.Pending = true
			o.Dropped = o.Kind == "R"
			p.AnyPending = true
		}
	}

	// Sessions, with overlapping operations of one session on separate
	// sub-rows.
	bySession := map[int64][]*op{}
	var sessions []int64
	for _, o := range opList {
		if _, ok := bySession[o.Client]; !ok {
			sessions = append(sessions, o.Client)
		}
		bySession[o.Client] = append(bySession[o.Client], o)
	}
	sort.Slice(sessions, func(i, j int) bool { return sessions[i] < sessions[j] })
	subRows := map[int64]int{}
	for _, s := range sessions {
		var ends []int
		for _, o := range bySession[s] {
			end := o.Ret
			if o.Pending {
				end = math.MaxInt
			}
			placed := false
			for k, last := range ends {
				if last < o.Call {
					o.Sub, ends[k], placed = k, end, true
					break
				}
			}
			if !placed {
				o.Sub = len(ends)
				ends = append(ends, end)
			}
		}
		subRows[s] = len(ends)
		if len(ends) > 1 {
			warn("session %d has operations that overlap in time; they are drawn on separate rows", s)
		}
	}
	p.Sessions = len(sessions)
	p.OpCount = len(opList)

	var nodes []int64
	seenNode := map[int64]bool{}
	for _, e := range sys {
		if !seenNode[e.Node] {
			seenNode[e.Node] = true
			nodes = append(nodes, e.Node)
		}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	p.Nodes = len(nodes)

	// The global time axis is linear in global time, and is offered only
	// when global time varies.
	n := len(in.Events)
	steps := make([]int64, 0, n)
	stepSeen := map[int64]bool{}
	gtMin, gtMax := int64(math.MaxInt64), int64(math.MinInt64)
	for _, e := range in.Events {
		if !stepSeen[e.Step] {
			stepSeen[e.Step] = true
			steps = append(steps, e.Step)
		}
		if e.GlobalTime < gtMin {
			gtMin = e.GlobalTime
		}
		if e.GlobalTime > gtMax {
			gtMax = e.GlobalTime
		}
	}
	sort.Slice(steps, func(i, j int) bool { return steps[i] < steps[j] })
	stepRank := map[int64]int{}
	for i, s := range steps {
		stepRank[s] = i
	}
	// Each distinct step is a point on the axis, and the last point is the
	// end that pending operations run to. The gaps between points start
	// narrow and widen only inside bars, until each bar is wide enough for
	// its label.
	cols := len(steps)
	gaps := make([]float64, cols)
	for i := range gaps {
		gaps[i] = minColumn
	}
	colOf := func(o *op) (int, int) {
		c := stepRank[in.Events[o.Call].Step]
		if o.Pending {
			return c, cols
		}
		return c, stepRank[in.Events[o.Ret].Step]
	}
	limit := maxLabel
	if len(opList) > 200 {
		limit = maxLabel / 2
	}
	byLength := append([]*op{}, opList...)
	sort.SliceStable(byLength, func(i, j int) bool {
		ci, ri := colOf(byLength[i])
		cj, rj := colOf(byLength[j])
		return ri-ci < rj-cj
	})
	for _, o := range byLength {
		c, r := colOf(o)
		if r <= c {
			continue
		}
		have := 0.0
		for j := c; j < r; j++ {
			have += gaps[j]
		}
		if need := math.Min(limit, labelPixels(o)); have < need {
			add := (need - have) / float64(r-c)
			for j := c; j < r; j++ {
				gaps[j] += add
			}
		}
	}
	centers := make([]float64, cols+1)
	centers[0] = padLeft + minColumn/2
	for i, w := range gaps {
		centers[i+1] = centers[i] + w
	}
	g := geom{hasGT: n > 0 && gtMax > gtMin}
	p.HasGT = g.hasGT
	first, endX := centers[0], centers[cols]
	tie := map[int64]int{}
	gtX := make([]float64, n)
	gtAt := func(t int64) float64 {
		return first + float64(t-gtMin)/float64(gtMax-gtMin)*(endX-first-minColumn)
	}
	if g.hasGT {
		for i, e := range in.Events {
			gtX[i] = gtAt(e.GlobalTime) + float64(tie[e.GlobalTime])*3
			tie[e.GlobalTime]++
		}
	}
	at := func(i int) pos { return pos{S: centers[stepRank[in.Events[i].Step]], G: gtX[i]} }
	end := pos{S: endX, G: endX}
	width := endX + padRight
	p.Width = num(width)
	p.LabelWidth = num(labelWidth)

	lastTick := math.Inf(-1)
	for i, s := range steps {
		if centers[i]-lastTick >= 40 {
			p.StepTicks = append(p.StepTicks, tickView{X: num(centers[i]), Label: strconv.FormatInt(s, 10)})
			lastTick = centers[i]
		}
	}
	if p.AnyPending {
		p.StepTicks = append(p.StepTicks, tickView{X: num(endX), Label: "end"})
	}
	if g.hasGT {
		for _, t := range niceTicks(gtMin, gtMax, 8) {
			p.GTTicks = append(p.GTTicks, tickView{X: num(gtAt(t)), Label: strconv.FormatInt(t, 10)})
		}
	}

	y := axisHeight
	nodeY := map[int64]float64{}
	nodeName := func(node int64) (string, string) {
		if in.NodeLabel != nil {
			if l, ok := in.NodeLabel(node); ok {
				return l, fmt.Sprintf("node %d", node)
			}
		}
		return fmt.Sprintf("Node %d", node), "node"
	}
	for i, nd := range nodes {
		label, sub := nodeName(nd)
		p.Lanes = append(p.Lanes, laneView{Label: label, Sub: sub, Y: num(y), H: num(nodeRow), TextY: num(y + 13), SubY: num(y + 25), Node: true, Stripe: i%2 == 1})
		nodeY[nd] = y
		y += nodeRow
	}
	if len(nodes) > 0 {
		y += laneGap
	}
	laneTop := map[int64]float64{}
	for i, s := range sessions {
		h := sessionRow * float64(subRows[s])
		p.Lanes = append(p.Lanes, laneView{Label: fmt.Sprintf("Client %d", s), Sub: fmt.Sprintf("session, %d ops", len(bySession[s])), Y: num(y), H: num(h), TextY: num(y + sessionRow/2 - 2), SubY: num(y + sessionRow/2 + 12), Stripe: i%2 == 1})
		laneTop[s] = y
		y += h
	}
	height := y + 18
	p.Height = num(height)
	barTop := func(o *op) float64 {
		return laneTop[o.Client] + float64(o.Sub)*sessionRow + (sessionRow-barHeight)/2
	}
	barX := func(o *op) (pos, pos) {
		x0 := at(o.Call)
		x1 := end
		if !o.Pending {
			x1 = at(o.Ret)
		}
		if x1.S-x0.S < minBar {
			x1.S = x0.S + minBar
		}
		if x1.G-x0.G < minBar {
			x1.G = x0.G + minBar
		}
		return x0, x1
	}
	center := func(o *op) pos {
		x0, x1 := barX(o)
		return pos{S: (x0.S + x1.S) / 2, G: (x0.G + x1.G) / 2}
	}

	downStart := map[int64]int{}
	for _, e := range sys {
		yy := nodeY[e.Node]
		label, _ := nodeName(e.Node)
		sv := sysView{Y: num(yy + nodeRow/2), Top: num(axisHeight), Bottom: num(height - 18)}
		x := at(e.Index)
		sv.At = g.attrs("transform", translate(x, pos{S: yy + nodeRow/2, G: yy + nodeRow/2}))
		sv.Line = g.attrs("x1", x, "x2", x)
		if e.Kind == "Crash" {
			sv.Class = "crash"
			sv.Tooltip = fmt.Sprintf("%s crashed at step %d (global time %d)", label, in.Events[e.Index].Step, in.Events[e.Index].GlobalTime)
			if _, down := downStart[e.Node]; !down {
				downStart[e.Node] = e.Index
			}
		} else {
			sv.Class = "recover"
			sv.Tooltip = fmt.Sprintf("%s recovered at step %d (global time %d)", label, in.Events[e.Index].Step, in.Events[e.Index].GlobalTime)
			if s, down := downStart[e.Node]; down {
				p.Downtimes = append(p.Downtimes, downView{Rect: g.attrs("x", at(s), "width", pos{S: at(e.Index).S - at(s).S, G: at(e.Index).G - at(s).G}), Y: num(yy + 7), H: num(nodeRow - 14)})
				delete(downStart, e.Node)
			}
		}
		p.SysMarks = append(p.SysMarks, sv)
	}
	for _, nd := range nodes {
		if s, down := downStart[nd]; down {
			p.Downtimes = append(p.Downtimes, downView{Rect: g.attrs("x", at(s), "width", pos{S: end.S - at(s).S, G: end.G - at(s).G}), Y: num(nodeY[nd] + 7), H: num(nodeRow - 14)})
		}
	}

	w := in.Witness
	lookup := func(id int64) *op {
		o, ok := ops[id]
		if !ok {
			warn("the witness names operation %d, which the history does not hold", id)
		}
		return o
	}
	descOf := func(id int64) string {
		if o, ok := ops[id]; ok {
			return o.desc()
		}
		return fmt.Sprintf("op %d", id)
	}
	inCycle := map[int64]bool{}
	blocked := map[int64]*Blocked{}
	valueRole := map[int64]string{}
	type edge struct {
		from, to int64
		rel      string
	}
	var edges []edge
	if w != nil {
		p.HasWitness = true
		p.Level = w.Level
		p.WitnessKind = w.Kind
		if w.Key != nil {
			p.Key, p.HasKey = keyText(*w.Key), true
		}
		// Replay the model over the witness order for the state after
		// each placement.
		state := map[string][]int64{}
		for i, id := range w.Order {
			o := lookup(id)
			if o == nil {
				continue
			}
			o.Seq = i + 1
			cur := state[o.Key]
			switch {
			case o.Kind == "W" && in.Model == "kv_rmw":
				cur = []int64{o.UID}
			case o.Kind == "W" || o.Kind == "M":
				cur = append(append([]int64{}, cur...), o.UID)
			}
			state[o.Key] = cur
			o.State, o.HasState = cur, true
			p.Order = append(p.Order, orderItem{Pos: i + 1, ID: id, Desc: o.desc(), State: keyText(o.Key) + " = " + listText(cur)})
		}
		switch w.Kind {
		case "order":
			p.OrderNote = "A total order that satisfies the claim. Each step shows the key's state after it."
		case "cycle":
			p.OrderNote = "Operations placed before the search stopped; the remaining ones lie on or behind a cycle of required orderings."
		case "frontier":
			p.OrderNote = "The longest partial order the search placed; every session head after it is blocked."
		}
		for _, e := range w.Cycle {
			lookup(e.From)
			lookup(e.To)
			inCycle[e.From], inCycle[e.To] = true, true
			edges = append(edges, edge{e.From, e.To, e.Relation})
			p.CycleList = append(p.CycleList, edgeItem{From: e.From, FromDesc: descOf(e.From), To: e.To, ToDesc: descOf(e.To), Relation: e.Relation})
		}
		for i := range w.Blocked {
			b := &w.Blocked[i]
			o := lookup(b.Op)
			blocked[b.Op] = b
			h := headView{Session: b.Session, ID: b.Op, Desc: descOf(b.Op), Reason: b.Reason}
			for _, a := range b.After {
				lookup(a.Op)
				edges = append(edges, edge{a.Op, b.Op, a.Relation})
				h.After = append(h.After, edgeItem{From: a.Op, FromDesc: descOf(a.Op), To: b.Op, ToDesc: descOf(b.Op), Relation: a.Relation})
			}
			if b.Reason == "value" {
				h.State, h.Observed = listText(b.State), listText(b.Observed)
				if o != nil {
					h.Key = keyText(o.Key)
				}
			}
			p.Heads = append(p.Heads, h)
		}
		if v := w.Value; v != nil {
			o := lookup(v.Op)
			valueRole[v.Op] = "op"
			vv := &valueView{ID: v.Op, Desc: descOf(v.Op), Reason: v.Reason, ReasonText: valueReasonText[v.Reason], UIDs: listText(v.UIDs)}
			listLabel := func(o *op) string {
				if o.Kind == "M" {
					return "prior"
				}
				return "observed"
			}
			if o != nil {
				vv.List, vv.ListLabel = listText(o.Value), listLabel(o)
			}
			if v.Other != nil {
				vv.HasOther, vv.Other, vv.OtherDesc = true, *v.Other, descOf(*v.Other)
				valueRole[*v.Other] = "other"
				if oo := lookup(*v.Other); oo != nil {
					vv.OtherList, vv.OtherLabel = listText(oo.Value), listLabel(oo)
				}
			}
			p.Value = vv
		}
	}

	for _, o := range opList {
		x0, x1 := barX(o)
		top := barTop(o)
		b := barView{ID: o.ID, Tooltip: tooltip(o, in), Y: num(top), LabelText: o.label()}
		var cls []string
		cls = append(cls, "k"+o.Kind)
		if o.Pending {
			cls = append(cls, "pending")
		}
		if o.Dropped {
			cls = append(cls, "dropped")
		}
		if inCycle[o.ID] {
			cls = append(cls, "incycle")
		}
		if p.HasKey && (!o.HasKey || keyText(o.Key) != p.Key) {
			cls = append(cls, "dim")
		}
		if r := valueRole[o.ID]; r != "" {
			cls = append(cls, "value-"+r)
			b.ValueRole = r
		}
		if o.Seq > 0 {
			cls = append(cls, "placed")
		}
		b.Class = strings.Join(cls, " ")
		w := pos{S: x1.S - x0.S, G: x1.G - x0.G}
		b.Rect = g.attrs("x", x0, "width", w)
		if o.Pending {
			b.Outline = g.attrs("d", [2]string{openPath(x0.S, x1.S, top), openPath(x0.G, x1.G, top)})
		}
		b.Lin = in.Claim[kindName[o.Kind]] == "linearizable"
		if b.Lin {
			p.AnyLin = true
			b.LinMark = g.attrs("x", x0)
		} else if o.Kind != "" {
			p.AnySeq = true
		}
		inset := 7.0
		if o.Seq > 0 {
			b.Seq = o.Seq
			b.SeqAt = g.attrs("transform", translate(pos{S: x0.S + 13, G: x0.G + 13}, pos{S: top + barHeight/2, G: top + barHeight/2}))
			inset = badgeSpace + 4
		}
		lw := pos{S: math.Max(0, w.S-inset-3), G: math.Max(0, w.G-inset-3)}
		b.LabelBox = g.attrs("x", pos{S: x0.S + inset, G: x0.G + inset}, "width", lw)
		if bl, ok := blocked[o.ID]; ok {
			b.Blocked = true
			b.BlockAt = g.attrs("x", pos{S: x0.S - 4, G: x0.G - 4}, "width", pos{S: w.S + 8, G: w.G + 8})
			b.BlockY = num(top - 4)
			if bl.Reason == "value" {
				b.Callout = keyText(o.Key) + " = " + listText(bl.State) + ", observed " + listText(bl.Observed)
			}
			b.CalloutAt = g.attrs("x", x0)
			b.CalloutY = num(top + barHeight + 16)
		}
		p.Bars = append(p.Bars, b)
	}

	pair := map[[2]int64]bool{}
	for _, e := range edges {
		pair[[2]int64{e.from, e.to}] = true
	}
	for _, e := range edges {
		a, b := ops[e.from], ops[e.to]
		if a == nil || b == nil {
			continue
		}
		reverse := pair[[2]int64{e.to, e.from}] && e.from != e.to
		av := arrowView{From: e.from, To: e.to, Relation: e.rel, Label: e.rel, Class: "rel-" + e.rel}
		pathS, lx, ly := arrowPath(center(a).S, barTop(a), center(b).S, barTop(b), e.from, e.to, reverse)
		pathG, gx, gy := arrowPath(center(a).G, barTop(a), center(b).G, barTop(b), e.from, e.to, reverse)
		av.Path = g.attrs("d", [2]string{pathS, pathG})
		av.LabelAt = g.attrs("transform", translate(pos{S: lx, G: gx}, pos{S: ly, G: gy}))
		lwid := 8 + 6.4*float64(len(e.rel))
		av.LabelW = num(lwid)
		av.LabelX = num(-lwid / 2)
		p.Arrows = append(p.Arrows, av)
	}
	if p.Value != nil && p.Value.HasOther {
		a, b := ops[p.Value.Other], ops[p.Value.ID]
		if a != nil && b != nil {
			pathS, lx, ly := arrowPath(center(a).S, barTop(a), center(b).S, barTop(b), a.ID, b.ID, false)
			pathG, gx, gy := arrowPath(center(a).G, barTop(a), center(b).G, barTop(b), a.ID, b.ID, false)
			lwid := 8 + 6.4*float64(len(p.Value.Reason))
			p.ValueLink = &arrowView{From: a.ID, To: b.ID, Label: p.Value.Reason, Path: g.attrs("d", [2]string{pathS, pathG}), LabelAt: g.attrs("transform", translate(pos{S: lx, G: gx}, pos{S: ly, G: gy})), LabelW: num(lwid), LabelX: num(-lwid / 2)}
		}
	}
	usedRel := map[string]bool{}
	for _, e := range edges {
		usedRel[e.rel] = true
	}
	for _, r := range relationOrder {
		if usedRel[r] {
			p.Relations = append(p.Relations, relView{Name: r, Text: relationText[r]})
		}
	}

	for _, o := range opList {
		r := rowView{ID: o.ID, Session: o.Client, Kind: kindVerb[o.Kind], Call: strconv.FormatInt(in.Events[o.Call].Step, 10), CallGT: strconv.FormatInt(in.Events[o.Call].GlobalTime, 10)}
		if r.Kind == "" {
			r.Kind = o.Action
		}
		if o.HasKey {
			r.Key = keyText(o.Key)
		}
		if o.HasUID {
			r.UID = strconv.FormatInt(o.UID, 10)
		}
		if o.HasValue {
			r.Result = listText(o.Value)
		}
		if o.Pending {
			r.Return, r.ReturnGT = "pending", "pending"
			if o.Dropped {
				r.Result = "dropped (pending read)"
			}
		} else {
			r.Return = strconv.FormatInt(in.Events[o.Ret].Step, 10)
			r.ReturnGT = strconv.FormatInt(in.Events[o.Ret].GlobalTime, 10)
		}
		if m := in.Claim[kindName[o.Kind]]; m != "" {
			r.Mark = m
		}
		if o.Seq > 0 {
			r.Seq = strconv.Itoa(o.Seq)
		}
		switch {
		case valueRole[o.ID] == "op" || inCycle[o.ID]:
			r.Class = "hl-bad"
		case blocked[o.ID] != nil:
			r.Class = "hl-warn"
		case valueRole[o.ID] == "other":
			r.Class = "hl-other"
		}
		p.Rows = append(p.Rows, r)
	}

	data, err := pageData(in, opList)
	if err != nil {
		return nil, err
	}
	p.Data = template.JS(data)
	return p, nil
}

func tooltip(o *op, in Input) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s\nsession %d", o.desc(), o.Client)
	call := in.Events[o.Call]
	fmt.Fprintf(&b, "\ninvoked at step %d (global time %d)", call.Step, call.GlobalTime)
	if o.Pending {
		b.WriteString("\npending: no response")
		if o.Dropped {
			b.WriteString("; a pending read is dropped by the check")
		}
	} else {
		ret := in.Events[o.Ret]
		fmt.Fprintf(&b, "\nreturned at step %d (global time %d)", ret.Step, ret.GlobalTime)
	}
	if m := in.Claim[kindName[o.Kind]]; m != "" {
		fmt.Fprintf(&b, "\nclaim: %s", m)
	}
	if o.Seq > 0 {
		fmt.Fprintf(&b, "\nwitness position %d, then %s = %s", o.Seq, keyText(o.Key), listText(o.State))
	}
	return b.String()
}

// openPath outlines a pending bar on its top, left and bottom sides only.
func openPath(x0, x1, top float64) string {
	return fmt.Sprintf("M%s %s H%s V%s H%s", num(x1), num(top), num(x0+5), num(top+barHeight), num(x1)) +
		fmt.Sprintf(" M%s %s Q%s %s %s %s", num(x0+5), num(top), num(x0), num(top), num(x0), num(top+5)) +
		fmt.Sprintf(" V%s Q%s %s %s %s", num(top+barHeight-5), num(x0), num(top+barHeight), num(x0+5), num(top+barHeight))
}

// arrowPath draws an edge between two bars and returns the point for its
// label. An edge whose reverse is also drawn is offset, or sent below the
// bars in one lane, so the two stay apart.
func arrowPath(ax, atop, bx, btop float64, from, to int64, reverse bool) (string, float64, float64) {
	abot, bbot := atop+barHeight, btop+barHeight
	switch {
	case from == to:
		d := fmt.Sprintf("M%s %s C%s %s %s %s %s %s", num(ax-9), num(atop), num(ax-26), num(atop-30), num(ax+26), num(atop-30), num(ax+9), num(atop-2))
		return d, ax, atop - 26
	case atop == btop:
		h := 20 + math.Min(10, math.Abs(bx-ax)*0.05)
		if reverse && from > to {
			d := fmt.Sprintf("M%s %s C%s %s %s %s %s %s", num(ax), num(abot), num(ax), num(abot+h), num(bx), num(bbot+h), num(bx), num(bbot+2))
			return d, (ax + bx) / 2, abot + 0.75*h
		}
		d := fmt.Sprintf("M%s %s C%s %s %s %s %s %s", num(ax), num(atop), num(ax), num(atop-h), num(bx), num(btop-h), num(bx), num(btop-2))
		return d, (ax + bx) / 2, atop - 0.75*h
	}
	off := 0.0
	if reverse {
		off = 10
		if from > to {
			off = -10
		}
	}
	ax, bx = ax+off, bx+off
	var y1, y2 float64
	if atop < btop {
		y1, y2 = abot, btop-2
	} else {
		y1, y2 = atop, bbot+2
	}
	mid := (y1 + y2) / 2
	d := fmt.Sprintf("M%s %s C%s %s %s %s %s %s", num(ax), num(y1), num(ax), num(mid), num(bx), num(mid), num(bx), num(y2))
	// The label sits near the target, so two edges that cross in the
	// middle keep their labels apart.
	lx, ly := cubic(0.7, ax, ax, bx, bx), cubic(0.7, y1, mid, mid, y2)
	return d, lx, ly
}

func cubic(t, p0, p1, p2, p3 float64) float64 {
	u := 1 - t
	return u*u*u*p0 + 3*u*u*t*p1 + 3*u*t*t*p2 + t*t*t*p3
}

// niceTicks returns up to about count round values covering [lo, hi].
func niceTicks(lo, hi int64, count int) []int64 {
	span := float64(hi - lo)
	raw := span / float64(count)
	mag := math.Pow(10, math.Floor(math.Log10(raw)))
	step := mag
	for _, m := range []float64{1, 2, 5, 10} {
		if m*mag >= raw {
			step = m * mag
			break
		}
	}
	st := int64(math.Max(1, step))
	var out []int64
	first := (lo + st - 1) / st * st
	if lo < 0 {
		first = lo / st * st
	}
	for t := first; t <= hi; t += st {
		out = append(out, t)
	}
	return out
}

type dataOp struct {
	ID         int64   `json:"id"`
	Session    int64   `json:"session"`
	Kind       string  `json:"kind"`
	Label      string  `json:"label"`
	Key        *string `json:"key"`
	UID        *int64  `json:"uid"`
	Value      []int64 `json:"value"`
	CallStep   int64   `json:"call_step"`
	ReturnStep *int64  `json:"return_step"`
	CallGT     int64   `json:"call_global_time"`
	ReturnGT   *int64  `json:"return_global_time"`
	Pending    bool    `json:"pending"`
	Dropped    bool    `json:"dropped"`
	Mark       string  `json:"mark"`
	Position   int     `json:"position"`
	StateAfter []int64 `json:"state_after"`
}

type dataBlock struct {
	Title   string            `json:"title"`
	Model   string            `json:"model"`
	Claim   map[string]string `json:"claim"`
	Verdict string            `json:"verdict"`
	Reason  string            `json:"reason"`
	Triage  string            `json:"triage"`
	Witness *Witness          `json:"witness"`
	Ops     []dataOp          `json:"ops"`
	Events  []Event           `json:"events"`
}

// pageData is the embedded JSON block. encoding/json escapes <, > and &, so
// the block cannot close its script element.
func pageData(in Input, opList []*op) ([]byte, error) {
	d := dataBlock{Title: in.Title, Model: in.Model, Claim: in.Claim, Verdict: in.Verdict, Reason: in.Reason, Triage: in.Triage, Witness: in.Witness, Events: in.Events}
	for _, o := range opList {
		do := dataOp{ID: o.ID, Session: o.Client, Kind: kindVerb[o.Kind], Label: o.label(), Value: o.Value,
			CallStep: in.Events[o.Call].Step, CallGT: in.Events[o.Call].GlobalTime, Pending: o.Pending, Dropped: o.Dropped,
			Mark: in.Claim[kindName[o.Kind]], Position: o.Seq}
		if o.HasKey {
			k := o.Key
			do.Key = &k
		}
		if o.HasUID {
			u := o.UID
			do.UID = &u
		}
		if !o.Pending {
			s, t := in.Events[o.Ret].Step, in.Events[o.Ret].GlobalTime
			do.ReturnStep, do.ReturnGT = &s, &t
		}
		if o.HasState {
			do.StateAfter = o.State
		}
		d.Ops = append(d.Ops, do)
	}
	return json.Marshal(d)
}
