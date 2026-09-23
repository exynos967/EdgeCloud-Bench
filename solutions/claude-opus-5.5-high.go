// Edge-Cloud collaborative scheduler (interactive).
//
// Structure of the policy:
//   - Decode requests circulate in "waves" (one D PRE group each). The number of
//     concurrently decoding requests (M) and of waves (W) come from a cycle-time model
//     built on the task-time table and the link formula (plan).
//   - The local computer E picks, among ready tasks, the one with the highest
//     delay-cost index (cost rate / processing time). Cost rates come from the current
//     gradient of the waiting-time score (TDR vs TPOT excess over their SLOs).
//   - Transfer completion times are predicted exactly (FIFO link), so E avoids starting
//     prefill work that would block a returning wave, and remotes split prefill into
//     pieces that end before decode data arrives, when that is worth it.
//   - Remote assignment weighs expected prefill wait against the decode-cycle impact,
//     restricted to a planned number of remotes (fewer remotes = fewer per-iteration
//     transfers when link latency dominates).
package main

import (
	"bufio"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

type Knobs struct {
	Eps         float64 // floor on normalized SLO excess used for gradients
	NewW        float64 // weight of members without a pending gap (first token)
	LavgPrior   float64 // prior mean output length before any FIN
	WaveGain    float64 // min relative model gain to add a wave / remote
	AdmitEps    float64 // decode admission: throughput slack vs best (<0 = admit all)
	FitBias     float64 // >1 favors prefill when it would delay a returning wave
	AssignDecW  float64 // weight of decode-cycle impact in remote assignment
	RhoWindow   float64 // min window (in SLO1 units) for the prefill load estimate
	SJF         bool    // prefill ordering by input length
	Predict     bool    // gate local prefill tasks by predicted wave returns
	PieceFit    bool    // split prefill so it ends before the next decode arrival
	RemoteCmu   bool    // remotes choose decode vs prefill by delay-cost index
	PlanRemotes bool    // restrict new assignments to a planned number of remotes
}

func DefaultKnobs() Knobs {
	return Knobs{Eps: 0.02, NewW: 0.01, LavgPrior: 64, WaveGain: 0.02, AdmitEps: 0.03, FitBias: 2,
		AssignDecW: 3, RhoWindow: 20, SJF: true, Predict: true, PieceFit: true, RemoteCmu: true, PlanRemotes: true}
}

const (
	stArrived = iota
	stPPreRun
	stPUp
	stProcReady
	stProcRun
	stPDown
	stPostReady
	stPostRun
	stDecReady
	stDPreRun
	stDUp
	stDProcReady
	stDProcRun
	stDDown
	stDPostReady
	stDPostRun
	stFin
)

const maxK = 8

type Req struct {
	lin       int
	arr       float64
	remote    int
	st        int
	nextLayer int
	wave      int
	tokens    int
	lastTok   float64
	eta       float64 // predicted completion of the request's in-flight transfer
	started   bool    // has entered decoding
}

// Wave is one D PRE group. Its members on a given remote always travel together
// (one UP transfer, one D PROC, one DOWN transfer), so per-remote state is read
// from a representative member.
type Wave struct {
	size    int
	arrived int
	members []int
	gap     int       // members that already produced a token
	cnt     [maxK]int // members per remote
	mg      [maxK]int // gap-bearing members per remote
	rep     [maxK]int // representative member per remote (-1 if none)
}

type pt struct{ x, y float64 }

type Table struct{ cols [6][]pt }

// lookup: piecewise-linear over listed sizes, clamped to the end values
func (tb *Table) lookup(c int, x float64) float64 {
	v := tb.cols[c]
	if x <= v[0].x {
		return v[0].y
	}
	n := len(v)
	if x >= v[n-1].x {
		return v[n-1].y
	}
	lo, hi := 0, n-1
	for hi-lo > 1 {
		mid := (lo + hi) / 2
		if v[mid].x <= x {
			lo = mid
		} else {
			hi = mid
		}
	}
	return v[lo].y + (v[hi].y-v[lo].y)*(x-v[lo].x)/(v[hi].x-v[lo].x)
}

const (
	cPPRE = iota
	cPPROC
	cPPOST
	cDPRE
	cDPROC
	cDPOST
)

type Sched struct {
	kn   Knobs
	K    int
	S    float64
	NL   int
	tab  Table
	reqs []Req
	now  float64
	sb   strings.Builder

	slo1, slo2 float64
	lat, bpb   float64 // link latency, ms per token

	// resources and predictions
	eBusy            bool
	rBusy            []bool
	rEnd             []float64 // predicted end of the task on each remote
	eEnd             float64
	upFree, downFree float64 // predicted time each link direction becomes idle
	xfers            int     // transfers in flight

	// ready queues
	arrivedQ   []int
	procReady  [][]int
	postReady  []int
	decCont    []int // decoding requests back from D POST
	decNew     []int // prefilled requests not yet admitted to decoding
	dprocReady [][]int
	dpostReady []int

	// waves
	waves         []Wave
	completeWaves []int
	wavesInFlight int
	flight        []int // ids of waves between D PRE and D POST

	// per-remote load
	backlog   []float64 // queued prefill work
	active    []int     // assigned unfinished requests
	activeDec []int     // prefilled unfinished requests
	running   int       // requests that started decoding and are unfinished
	prefWork  float64   // remote prefill work of all arrivals
	tFirst    float64
	nArr      int

	// caches
	decVer, planVer, planM, planW, planCalls int
	dcVer, dcCntVer                          int
	dcCnt                                    []int
	allowBuf                                 []bool
	cntBuf                                   []float64

	// running metric estimates
	pendCnt, nTdrDone             int
	pendArrSum, sumTdrDone        float64
	gapCnt, actCnt, finCnt        int
	gapSum, actLastSum, finTokSum float64
}

func pf(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func pi(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func NewSched(header []string, kn Knobs) *Sched {
	p := strings.Fields(header[0])
	s := &Sched{kn: kn}
	s.K = pi(p[0])
	s.S = pf(p[1])
	s.lat = pf(p[2])
	s.bpb = 8 * pf(p[4]) / (pf(p[3]) * 1e6)
	s.NL = pi(p[5])
	q := strings.Fields(header[1])
	s.slo1 = pf(q[0])
	s.slo2 = pf(q[1])
	s.tFirst = -1
	n := pi(strings.TrimSpace(header[2]))
	for i := 0; i < n; i++ {
		f := strings.Fields(header[3+i])
		b := pf(f[0])
		for c := 0; c < 6; c++ {
			v := pf(f[1+c])
			if v >= 0 {
				s.tab.cols[c] = append(s.tab.cols[c], pt{b, v})
			}
		}
	}
	for c := 0; c < 6; c++ {
		col := s.tab.cols[c]
		sort.Slice(col, func(a, b int) bool { return col[a].x < col[b].x })
	}
	s.rBusy = make([]bool, s.K)
	s.rEnd = make([]float64, s.K)
	s.procReady = make([][]int, s.K)
	s.dprocReady = make([][]int, s.K)
	s.backlog = make([]float64, s.K)
	s.active = make([]int, s.K)
	s.activeDec = make([]int, s.K)
	s.dcCnt = make([]int, s.K)
	s.allowBuf = make([]bool, s.K)
	s.cntBuf = make([]float64, s.K)
	s.dcCntVer = -1
	s.planVer = -1
	return s
}

// ---------------------------------------------------------------- events

func (s *Sched) applyEvent(t float64, w []string, fins *[]int) {
	if len(w) == 0 {
		return
	}
	switch w[0] {
	case "ARR":
		rid := pi(w[1])
		for len(s.reqs) <= rid {
			s.reqs = append(s.reqs, Req{remote: -1})
		}
		s.reqs[rid] = Req{lin: pi(w[2]), arr: t, remote: -1, st: stArrived}
		s.arrivedQ = append(s.arrivedQ, rid)
		s.pendCnt++
		s.nArr++
		s.pendArrSum += t
		s.prefWork += s.S + s.tab.lookup(cPPROC, float64(s.reqs[rid].lin))
		if s.tFirst < 0 {
			s.tFirst = t
		}
	case "FIN":
		*fins = append(*fins, pi(w[1]))
	case "TDN":
		if w[1] == "E" {
			s.eBusy = false
		} else {
			s.rBusy[pi(w[1][1:])] = false
		}
		switch w[2] + w[3] {
		case "PPRE":
			r := &s.reqs[pi(w[5])]
			r.st = stPUp
			r.eta = s.enqUp(t, float64(r.lin))
		case "PPROC":
			le := pi(w[5])
			rid := pi(w[7])
			r := &s.reqs[rid]
			r.nextLayer = le
			if le == s.NL {
				r.st = stPDown
				r.eta = s.enqDown(t, float64(r.lin))
			} else {
				r.st = stProcReady
				s.procReady[r.remote] = append(s.procReady[r.remote], rid)
			}
		case "PPOST":
			rid := pi(w[5])
			r := &s.reqs[rid]
			r.st = stDecReady
			s.pendCnt--
			s.pendArrSum -= r.arr
			s.sumTdrDone += t - r.arr
			s.nTdrDone++
			s.activeDec[r.remote]++
			s.decVer++
			s.decNew = append(s.decNew, rid)
		case "DPRE":
			m := pi(w[5])
			var cnt [maxK]int
			for i := 0; i < m; i++ {
				r := &s.reqs[pi(w[6+i])]
				r.st = stDUp
				cnt[r.remote]++
			}
			// one UP transfer per remote, enqueued in increasing remote index
			var eta [maxK]float64
			for k := 0; k < s.K; k++ {
				if cnt[k] > 0 {
					eta[k] = s.enqUp(t, float64(cnt[k]))
				}
			}
			for i := 0; i < m; i++ {
				r := &s.reqs[pi(w[6+i])]
				r.eta = eta[r.remote]
			}
		case "DPROC":
			m := pi(w[5])
			eta := s.enqDown(t, float64(m))
			for i := 0; i < m; i++ {
				r := &s.reqs[pi(w[6+i])]
				r.st = stDDown
				r.eta = eta
			}
		case "DPOST":
			m := pi(w[5])
			seen := map[int]bool{}
			for i := 0; i < m; i++ {
				rid := pi(w[6+i])
				r := &s.reqs[rid]
				if r.tokens > 0 {
					s.gapSum += t - r.lastTok
					s.gapCnt++
					s.actLastSum -= r.lastTok
				} else {
					s.actCnt++
				}
				r.lastTok = t
				s.actLastSum += t
				r.tokens++
				r.st = stDecReady
				seen[r.wave] = true
				s.decCont = append(s.decCont, rid)
				s.dcVer++
			}
			s.wavesInFlight -= len(seen)
			k := 0
			for _, fw := range s.flight {
				if !seen[fw] {
					s.flight[k] = fw
					k++
				}
			}
			s.flight = s.flight[:k]
		}
	case "XDN":
		s.xfers--
		up := w[1] == "UP"
		rm := pi(w[2])
		pre := w[4] == "PRE"
		m := pi(w[5])
		for i := 0; i < m; i++ {
			rid := pi(w[6+i])
			r := &s.reqs[rid]
			switch {
			case up && pre:
				r.st = stProcReady
				s.procReady[rm] = append(s.procReady[rm], rid)
			case !up && pre:
				r.st = stPostReady
				s.postReady = append(s.postReady, rid)
			case up && !pre:
				r.st = stDProcReady
				s.dprocReady[rm] = append(s.dprocReady[rm], rid)
			default:
				r.st = stDPostReady
				s.dpostReady = append(s.dpostReady, rid)
				wa := &s.waves[r.wave]
				wa.arrived++
				if wa.arrived == wa.size {
					s.completeWaves = append(s.completeWaves, r.wave)
				}
			}
		}
	}
}

func (s *Sched) enqUp(t, l float64) float64 {
	s.xfers++
	s.upFree = math.Max(s.upFree, t) + s.lat + l*s.bpb
	return s.upFree
}

func (s *Sched) enqDown(t, l float64) float64 {
	s.xfers++
	s.downFree = math.Max(s.downFree, t) + s.lat + l*s.bpb
	return s.downFree
}

// OnFrame processes one frame and returns the response text.
func (s *Sched) OnFrame(t float64, events [][]string) string {
	s.now = t
	var fins []int
	for _, w := range events {
		s.applyEvent(t, w, &fins)
	}
	if len(fins) > 0 {
		for _, rid := range fins {
			r := &s.reqs[rid]
			r.st = stFin
			s.actCnt--
			s.actLastSum -= r.lastTok
			s.finCnt++
			s.finTokSum += float64(r.tokens)
			s.active[r.remote]--
			s.activeDec[r.remote]--
			s.running--
			s.decVer++
		}
		k := 0
		for _, rid := range s.decCont {
			if s.reqs[rid].st != stFin {
				s.decCont[k] = rid
				k++
			}
		}
		s.decCont = s.decCont[:k]
		s.dcVer++
	}
	cmds := s.decide(t)
	if len(cmds) == 0 && s.quiet() && s.nArr > s.finCnt {
		cmds = s.forceProgress(t)
	}
	s.sb.Reset()
	s.sb.WriteString(strconv.Itoa(len(cmds)))
	s.sb.WriteByte('\n')
	for _, c := range cmds {
		s.sb.WriteString(c)
		s.sb.WriteByte('\n')
	}
	return s.sb.String()
}

// ---------------------------------------------------------------- helpers

// prefill ordering: shorter input first (SJF), then arrival order
func (s *Sched) prefLess(a, b int) bool {
	if s.kn.SJF && s.reqs[a].lin != s.reqs[b].lin {
		return s.reqs[a].lin < s.reqs[b].lin
	}
	return a < b
}

func (s *Sched) bestOf(v []int) int {
	rid := v[0]
	for _, r := range v {
		if s.prefLess(r, rid) {
			rid = r
		}
	}
	return rid
}

func (s *Sched) popBest(a *[]int) int {
	v := *a
	bi := 0
	for i := 1; i < len(v); i++ {
		if s.prefLess(v[i], v[bi]) {
			bi = i
		}
	}
	r := v[bi]
	v[bi] = v[len(v)-1]
	*a = v[:len(v)-1]
	return r
}

func groupStr(prefix string, g []int) string {
	var b strings.Builder
	b.WriteString(prefix)
	b.WriteByte(' ')
	b.WriteString(strconv.Itoa(len(g)))
	for _, r := range g {
		b.WriteByte(' ')
		b.WriteString(strconv.Itoa(r))
	}
	return b.String()
}

func procCmd(k, ls, le, rid int) string {
	return "C" + strconv.Itoa(k) + " P PROC " + strconv.Itoa(ls) + " " + strconv.Itoa(le) + " " + strconv.Itoa(k) + " " + strconv.Itoa(rid)
}

// metric estimates at time t (pending requests count with their current wait)
func (s *Sched) estimates(t float64) (e1, e2, lavg float64) {
	tdrN := float64(s.nTdrDone + s.pendCnt)
	tdr := 0.0
	if tdrN > 0 {
		tdr = (s.sumTdrDone + float64(s.pendCnt)*t - s.pendArrSum) / tdrN
	}
	gN := float64(s.gapCnt + s.actCnt)
	tpot := 0.0
	if gN > 0 {
		tpot = (s.gapSum + float64(s.actCnt)*t - s.actLastSum) / gN
	}
	e1 = tdr/s.slo1 - 1
	e2 = tpot/s.slo2 - 1
	if s.finCnt > 0 {
		lavg = s.finTokSum / float64(s.finCnt)
	} else {
		lavg = s.kn.LavgPrior
	}
	return
}

// per-ms delay cost of one prefill request (w1) and of one decoding member's gap (w2)
func (s *Sched) weights(t float64) (float64, float64) {
	e1, e2, lavg := s.estimates(t)
	g1 := math.Max(e1, s.kn.Eps)
	g2 := math.Max(e2, s.kn.Eps)
	return g1 / s.slo1, g2 / (math.Max(lavg-1, 1) * s.slo2)
}

// ---------------------------------------------------------------- waves and predictions

func (s *Sched) newWave(g []int) int {
	wid := len(s.waves)
	wa := Wave{size: len(g), members: g}
	for k := range wa.rep {
		wa.rep[k] = -1
	}
	for _, rid := range g {
		r := &s.reqs[rid]
		wa.cnt[r.remote]++
		wa.rep[r.remote] = rid
		if r.tokens > 0 {
			wa.mg[r.remote]++
			wa.gap++
		}
	}
	s.waves = append(s.waves, wa)
	s.wavesInFlight++
	s.flight = append(s.flight, wid)
	return wid
}

// predicted time at which every member of wave w is back at the local computer
func (s *Sched) waveReturn(w int, t float64) float64 {
	wa := &s.waves[w]
	ret := t
	down := math.Max(s.downFree, t)
	for k := 0; k < s.K; k++ {
		if wa.cnt[k] == 0 {
			continue
		}
		m := float64(wa.cnt[k])
		r := &s.reqs[wa.rep[k]]
		var arrive float64
		switch r.st {
		case stDPostReady, stDPostRun:
			arrive = t
		case stDDown:
			arrive = math.Max(t, r.eta)
		default:
			start := t
			if r.st == stDUp || r.st == stDPreRun {
				start = math.Max(start, r.eta)
			}
			end := s.rEnd[k]
			if r.st != stDProcRun {
				end = math.Max(start, s.rEnd[k]) + s.S + s.tab.lookup(cDPROC, m)
			}
			down = math.Max(down, end) + s.lat + m*s.bpb
			arrive = down
		}
		ret = math.Max(ret, arrive)
	}
	return ret
}

// predicted earliest arrival of decode work at remote k:
// time, member count, and members that already produced a token
func (s *Sched) nextDecArrival(k int, t float64) (float64, int, int) {
	best := math.Inf(1)
	cnt, cg := 0, 0
	for _, w := range s.flight {
		wa := &s.waves[w]
		m, mg := wa.cnt[k], wa.mg[k]
		if m == 0 {
			continue
		}
		st, eta := s.reqs[wa.rep[k]].st, s.reqs[wa.rep[k]].eta
		var a float64
		switch st {
		case stDUp:
			a = eta
		case stDPreRun:
			a = math.Max(s.upFree, s.eEnd) + s.lat + float64(m)*s.bpb
		case stDProcReady:
			a = t
		default:
			fs := float64(wa.size)
			a = s.waveReturn(w, t) + 2*s.S + s.tab.lookup(cDPOST, fs) + s.tab.lookup(cDPRE, fs) + s.lat + float64(m)*s.bpb
		}
		if a < best {
			best, cnt, cg = a, m, mg
		}
	}
	if len(s.decCont) > 0 {
		if s.dcVer != s.dcCntVer {
			s.dcCntVer = s.dcVer
			for j := range s.dcCnt {
				s.dcCnt[j] = 0
			}
			for _, rid := range s.decCont {
				s.dcCnt[s.reqs[rid].remote]++
			}
		}
		m := s.dcCnt[k]
		if m > 0 {
			a := math.Max(t, s.eEnd) + s.S + s.tab.lookup(cDPRE, float64(len(s.decCont))) + s.lat + float64(m)*s.bpb
			if a < best {
				best, cnt, cg = a, m, m
			}
		}
	}
	return best, cnt, cg
}

// whether starting a local prefill task of length p now is worth delaying returning waves
func (s *Sched) prefillFits(t, p, w1, w2 float64) bool {
	over := 0.0
	rmin := math.Inf(1)
	mTot := 0
	for _, w := range s.flight {
		wa := &s.waves[w]
		r := s.waveReturn(w, t)
		if r < t+p {
			over += float64(wa.gap) * (t + p - r)
			rmin = math.Min(rmin, r)
			mTot += wa.size
		}
	}
	if over == 0 {
		return true
	}
	fm := float64(mTot)
	wait := (rmin - t) + 2*s.S + s.tab.lookup(cDPOST, fm) + s.tab.lookup(cDPRE, fm)
	return w2*over <= s.kn.FitBias*w1*wait
}

// ---------------------------------------------------------------- decode planning

// cycle time of one decode loop with the given per-remote member counts in w waves
func (s *Sched) cycleCounts(cnt []float64, w int) float64 {
	fw := float64(w)
	tot := 0.0
	for _, c := range cnt {
		tot += c
	}
	if tot == 0 {
		return 0
	}
	m := math.Ceil(tot / fw)
	dE := 2*s.S + s.tab.lookup(cDPRE, m) + s.tab.lookup(cDPOST, m)
	up, dR, mmax := 0.0, 0.0, 0.0
	for _, c := range cnt {
		if c == 0 {
			continue
		}
		x := math.Ceil(c / fw)
		up += s.lat + x*s.bpb
		dR = math.Max(dR, s.S+s.tab.lookup(cDPROC, x))
		mmax = math.Max(mmax, x)
	}
	chain := dE + up + dR + s.lat + mmax*s.bpb
	return math.Max(chain, fw*math.Max(dE, math.Max(up, dR)))
}

// cycle time with M running members in w waves, spread over remotes like the
// current decode population
func (s *Sched) cycleModel(M, w int) float64 {
	tot := 0
	for _, m := range s.activeDec {
		tot += m
	}
	fw := float64(w)
	m := math.Ceil(float64(M) / fw)
	dE := 2*s.S + s.tab.lookup(cDPRE, m) + s.tab.lookup(cDPOST, m)
	up, dR, mmax := 0.0, 0.0, 0.0
	for k := 0; k < s.K; k++ {
		var share float64
		if tot > 0 {
			share = float64(s.activeDec[k]) / float64(tot)
		} else {
			share = 1 / float64(s.K)
		}
		if share == 0 {
			continue
		}
		x := math.Ceil(float64(M) * share / fw)
		up += s.lat + x*s.bpb
		dR = math.Max(dR, s.S+s.tab.lookup(cDPROC, x))
		mmax = math.Max(mmax, x)
	}
	chain := dE + up + dR + s.lat + mmax*s.bpb
	return math.Max(chain, fw*math.Max(dE, math.Max(up, dR)))
}

// plan chooses the number of running decode members and waves: the smallest M whose
// modeled throughput is within AdmitEps of the best reachable one.
func (s *Sched) plan() (int, int) {
	s.planCalls++
	if s.planVer == s.decVer && s.planCalls < 64 {
		return s.planM, s.planW
	}
	s.planCalls = 0
	s.planVer = s.decVer
	mall := 0
	for _, m := range s.activeDec {
		mall += m
	}
	if mall == 0 {
		s.planM, s.planW = 1, 1
		return 1, 1
	}
	var grid []int
	for m := 1; m < mall; {
		grid = append(grid, m)
		nm := int(float64(m) * 1.12)
		if nm <= m {
			nm = m + 1
		}
		m = nm
	}
	grid = append(grid, mall)
	th := make([]float64, len(grid))
	bw := make([]int, len(grid))
	best := 0.0
	for i, m := range grid {
		for w := 1; w <= 4 && w <= m; w++ {
			v := float64(m) / s.cycleModel(m, w)
			if v > th[i]*(1+s.kn.WaveGain) {
				th[i], bw[i] = v, w
			}
		}
		best = math.Max(best, th[i])
	}
	s.planM, s.planW = mall, bw[len(grid)-1]
	if s.kn.AdmitEps >= 0 {
		for i, m := range grid {
			if th[i] >= (1-s.kn.AdmitEps)*best {
				s.planM, s.planW = m, bw[i]
				break
			}
		}
	}
	return s.planM, s.planW
}

// how many waiting requests may start decoding now
func (s *Sched) admitRoom() int {
	m, _ := s.plan()
	if s.running == 0 && m < 1 {
		m = 1
	}
	return m - s.running
}

// ---------------------------------------------------------------- decisions

func (s *Sched) decide(t float64) []string {
	var cmds []string
	if !s.eBusy {
		if c := s.pickE(t); c != "" {
			s.eBusy = true
			cmds = append(cmds, c)
		}
	}
	for k := 0; k < s.K; k++ {
		if s.rBusy[k] {
			continue
		}
		decodeFirst := len(s.dprocReady[k]) > 0
		if decodeFirst && s.kn.RemoteCmu && len(s.procReady[k]) > 0 {
			// decode vs prefill on this remote by delay-cost per unit of work
			w1, w2 := s.weights(t)
			mg := 0
			for _, rid := range s.dprocReady[k] {
				if s.reqs[rid].tokens > 0 {
					mg++
				}
			}
			m := float64(len(s.dprocReady[k]))
			idxD := (float64(mg) + s.kn.NewW*m) * w2 / (s.S + s.tab.lookup(cDPROC, m))
			rq := &s.reqs[s.bestOf(s.procReady[k])]
			rem := float64(s.NL-rq.nextLayer) / float64(s.NL) * s.tab.lookup(cPPROC, float64(rq.lin))
			if w1/(s.S+rem) > idxD {
				decodeFirst = false
			}
		}
		if decodeFirst {
			g := s.dprocReady[k]
			s.dprocReady[k] = nil
			for _, r := range g {
				s.reqs[r].st = stDProcRun
			}
			cmds = append(cmds, groupStr("C"+strconv.Itoa(k)+" D PROC "+strconv.Itoa(k), g))
			s.rBusy[k] = true
			s.rEnd[k] = t + s.S + s.tab.lookup(cDPROC, float64(len(g)))
			continue
		}
		if len(s.procReady[k]) > 0 {
			if c := s.startPiece(k, t); c != "" {
				cmds = append(cmds, c)
			}
		}
	}
	return cmds
}

// startPiece runs the next prefill piece on remote k, sized to end before the next
// decode arrival when the decode delay would cost more than splitting.
func (s *Sched) startPiece(k int, t float64) string {
	rid := s.popBest(&s.procReady[k])
	r := &s.reqs[rid]
	ls := r.nextLayer
	le := s.NL
	per := s.tab.lookup(cPPROC, float64(r.lin)) / float64(s.NL)
	if s.kn.PieceFit && s.running > 0 {
		a, m, mg := s.nextDecArrival(k, t)
		wr := float64(le-ls) * per
		if m > 0 && a < t+s.S+wr {
			w1, w2 := s.weights(t)
			q := float64(1 + len(s.procReady[k]))
			costFull := float64(mg) * w2 * (t + s.S + wr - a)
			costSplit := w1 * q * (2*s.S + s.tab.lookup(cDPROC, float64(m)))
			if costFull > costSplit {
				n := int((a - t - s.S) / per)
				if n < 1 {
					// a single layer still overruns the arrival: run it or wait for the decode
					cost1 := float64(mg) * w2 * (t + s.S + per - a)
					costIdle := w1 * q * (a - t)
					if costIdle < cost1 && (len(s.flight) > 0 || s.eBusy) {
						s.procReady[k] = append(s.procReady[k], rid)
						return ""
					}
					n = 1
				}
				le = ls + n
			}
		}
	}
	s.backlog[k] -= float64(le-ls) * per
	s.rEnd[k] = t + s.S + float64(le-ls)*per
	r.st = stProcRun
	s.rBusy[k] = true
	return procCmd(k, ls, le, rid)
}

// pickE chooses the local computer's task by delay-cost index.
func (s *Sched) pickE(t float64) string {
	w1, w2 := s.weights(t)
	best := -1.0
	choice := -1
	if len(s.completeWaves) > 0 {
		m, mg := 0, 0
		for _, w := range s.completeWaves {
			m += s.waves[w].size
			mg += s.waves[w].gap
		}
		idx := (float64(mg)*w2 + float64(m)*w2*s.kn.NewW) / (s.S + s.tab.lookup(cDPOST, float64(m)))
		if idx > best {
			best, choice = idx, 0
		}
	}
	if len(s.decCont) > 0 || (len(s.decNew) > 0 && s.wavesInFlight < s.planWaves() && s.admitRoom() > 0) {
		mc := len(s.decCont)
		room := s.admitRoom()
		if room < 0 {
			room = 0
		}
		if room > len(s.decNew) {
			room = len(s.decNew)
		}
		fm := float64(mc + room)
		idx := (float64(mc)*w2 + fm*w2*s.kn.NewW) / (2*s.S + s.tab.lookup(cDPRE, fm) + s.tab.lookup(cDPOST, fm))
		if idx > best {
			best, choice = idx, 1
		}
	}
	decChoice := choice
	postRid, preRid := -1, -1
	if len(s.postReady) > 0 {
		postRid = s.bestOf(s.postReady)
		idx := w1 / (s.S + s.tab.lookup(cPPOST, float64(s.reqs[postRid].lin)))
		if idx > best {
			best, choice = idx, 2
		}
	}
	if len(s.arrivedQ) > 0 {
		preRid = s.bestOf(s.arrivedQ)
		l := float64(s.reqs[preRid].lin)
		idx := w1 / (2*s.S + s.tab.lookup(cPPRE, l) + s.tab.lookup(cPPOST, l))
		if idx > best {
			best, choice = idx, 3
		}
	}
	if (choice == 2 || choice == 3) && s.kn.Predict && s.wavesInFlight > 0 {
		var p float64
		if choice == 2 {
			p = s.S + s.tab.lookup(cPPOST, float64(s.reqs[postRid].lin))
		} else {
			p = s.S + s.tab.lookup(cPPRE, float64(s.reqs[preRid].lin))
		}
		if !s.prefillFits(t, p, w1, w2) {
			// try the other prefill kind, then decode work, before idling
			alt := -1
			if choice == 3 && postRid >= 0 {
				alt = 2
				p = s.S + s.tab.lookup(cPPOST, float64(s.reqs[postRid].lin))
			} else if choice == 2 && preRid >= 0 {
				alt = 3
				p = s.S + s.tab.lookup(cPPRE, float64(s.reqs[preRid].lin))
			}
			if alt >= 0 && s.prefillFits(t, p, w1, w2) {
				choice = alt
			} else if decChoice >= 0 {
				choice = decChoice
			} else {
				return ""
			}
		}
	}
	switch choice {
	case 0:
		return s.tryDPost()
	case 1:
		return s.tryDPre()
	case 2:
		return s.tryPPost()
	case 3:
		return s.tryPPre()
	}
	return ""
}

func (s *Sched) planWaves() int {
	_, w := s.plan()
	return w
}

// tryDPost posts every complete wave together (merging them).
func (s *Sched) tryDPost() string {
	if len(s.completeWaves) == 0 {
		return ""
	}
	ws := map[int]bool{}
	for _, w := range s.completeWaves {
		ws[w] = true
	}
	s.completeWaves = s.completeWaves[:0]
	var g []int
	k := 0
	for _, rid := range s.dpostReady {
		if ws[s.reqs[rid].wave] {
			g = append(g, rid)
			s.reqs[rid].st = stDPostRun
		} else {
			s.dpostReady[k] = rid
			k++
		}
	}
	s.dpostReady = s.dpostReady[:k]
	s.eEnd = s.now + s.S + s.tab.lookup(cDPOST, float64(len(g)))
	return groupStr("E D POST -1", g)
}

// tryDPre starts the next wave: returning members plus admitted new ones, split into a
// chunk (balanced over remotes) when the plan wants more waves than are in flight.
func (s *Sched) tryDPre() string {
	planM, capW := s.plan()
	room := s.admitRoom()
	if len(s.decCont) == 0 && (len(s.decNew) == 0 || s.wavesInFlight >= capW || room <= 0) {
		return ""
	}
	g := append([]int(nil), s.decCont...)
	s.decCont = nil
	s.dcVer++
	if room > 0 {
		n := room
		if n > len(s.decNew) {
			n = len(s.decNew)
		}
		g = append(g, s.decNew[:n]...)
		s.decNew = append([]int(nil), s.decNew[n:]...)
	}
	if s.wavesInFlight+1 < capW && len(g) >= 2 {
		chunk := (planM + capW - 1) / capW
		if chunk < len(g) {
			sort.Slice(g, func(a, b int) bool {
				ra, rb := s.reqs[g[a]].remote, s.reqs[g[b]].remote
				if ra != rb {
					return ra < rb
				}
				return g[a] < g[b]
			})
			var take []int
			step := float64(len(g)) / float64(chunk)
			pos := 0.0
			used := make([]bool, len(g))
			for len(take) < chunk {
				i := int(pos)
				if i >= len(g) {
					break
				}
				used[i] = true
				take = append(take, g[i])
				pos += step
			}
			for i, r := range g {
				if !used[i] {
					if s.reqs[r].tokens > 0 {
						s.decCont = append(s.decCont, r)
						s.dcVer++
					} else {
						s.decNew = append(s.decNew, r)
					}
				}
			}
			g = take
		}
	}
	s.startWave(g)
	s.eEnd = s.now + s.S + s.tab.lookup(cDPRE, float64(len(g)))
	return groupStr("E D PRE -1", g)
}

func (s *Sched) startWave(g []int) {
	wid := s.newWave(g)
	for _, r := range g {
		q := &s.reqs[r]
		q.st = stDPreRun
		q.wave = wid
		if !q.started {
			q.started = true
			s.running++
		}
	}
}

func (s *Sched) tryPPost() string {
	if len(s.postReady) == 0 {
		return ""
	}
	rid := s.popBest(&s.postReady)
	s.reqs[rid].st = stPostRun
	s.eEnd = s.now + s.S + s.tab.lookup(cPPOST, float64(s.reqs[rid].lin))
	return "E P POST " + strconv.Itoa(s.reqs[rid].remote) + " " + strconv.Itoa(rid)
}

func (s *Sched) tryPPre() string {
	if len(s.arrivedQ) == 0 {
		return ""
	}
	rid := s.popBest(&s.arrivedQ)
	r := &s.reqs[rid]
	best := s.chooseRemote(rid)
	s.backlog[best] += s.tab.lookup(cPPROC, float64(r.lin))
	s.active[best]++
	r.remote = best
	r.st = stPPreRun
	s.eEnd = s.now + s.S + s.tab.lookup(cPPRE, float64(r.lin))
	return "E P PRE " + strconv.Itoa(best) + " " + strconv.Itoa(rid)
}

// ---------------------------------------------------------------- remote assignment

// planRemotes picks how many remotes new requests should use: the count minimizing the
// modeled decode cycle, with remote time inflated by the prefill load share.
func (s *Sched) planRemotes() int {
	m, w := s.plan()
	mall := 0
	for _, c := range s.activeDec {
		mall += c
	}
	if m < mall {
		m = mall
	}
	m += len(s.arrivedQ) + 1
	elapsed := math.Max(s.now-s.tFirst, s.kn.RhoWindow*s.slo1)
	rho := s.prefWork / elapsed
	best, bv := s.K, math.Inf(1)
	fw := float64(w)
	for kp := s.K; kp >= 1; kp-- {
		util := rho / float64(kp)
		if util >= 0.9 {
			break
		}
		per := math.Ceil(float64(m) / float64(kp) / fw)
		mm := math.Ceil(float64(m) / fw)
		dE := 2*s.S + s.tab.lookup(cDPRE, mm) + s.tab.lookup(cDPOST, mm)
		up := float64(kp) * (s.lat + per*s.bpb)
		dR := (s.S + s.tab.lookup(cDPROC, per)) / (1 - util)
		chain := dE + up + dR + s.lat + per*s.bpb
		c := math.Max(chain, fw*math.Max(dE, math.Max(up, dR)))
		if c < bv*(1-s.kn.WaveGain) {
			best, bv = kp, c
		}
	}
	return best
}

// chooseRemote picks the remote for a new request (no state change): the planned
// remote set, then minimal marginal cost.
func (s *Sched) chooseRemote(rid int) int {
	pp := s.tab.lookup(cPPROC, float64(s.reqs[rid].lin))
	allowed := s.allowBuf
	for k := range allowed {
		allowed[k] = true
	}
	if s.kn.PlanRemotes && s.K > 1 {
		kp := s.planRemotes()
		// keep the kp remotes carrying most requests
		idx := make([]int, s.K)
		for k := range idx {
			idx[k] = k
		}
		sort.Slice(idx, func(a, b int) bool {
			x, y := idx[a], idx[b]
			if s.active[x] != s.active[y] {
				return s.active[x] > s.active[y]
			}
			return x < y
		})
		for k := range allowed {
			allowed[k] = false
		}
		for i := 0; i < kp; i++ {
			allowed[idx[i]] = true
		}
	}
	mplan, wplan := s.plan()
	for k := 0; k < s.K; k++ {
		s.cntBuf[k] = float64(s.activeDec[k])
	}
	cyc0 := s.cycleCounts(s.cntBuf, wplan)
	w1, w2 := s.weights(s.now)
	_, _, lavg := s.estimates(s.now)
	best := 0
	bk := math.Inf(1)
	for k := 0; k < s.K; k++ {
		if !allowed[k] {
			continue
		}
		if key := s.assignCost(k, pp, cyc0, w1, w2, lavg, mplan, wplan); key < bk {
			bk = key
			best = k
		}
	}
	return best
}

// marginal cost of assigning a new request with prefill work pp to remote k:
// its expected prefill wait there plus the decode-cycle increase over its lifetime
func (s *Sched) assignCost(k int, pp, cyc0, w1, w2, lavg float64, mplan, wplan int) float64 {
	wait := math.Max(0, s.rEnd[k]-s.now) + s.backlog[k] + pp
	if cyc0 > 0 && s.activeDec[k] > 0 {
		rho := (s.S + s.tab.lookup(cDPROC, float64(s.activeDec[k]))) / cyc0
		wait /= math.Max(0.1, 1-rho)
	}
	s.cntBuf[k]++
	c1 := s.cycleCounts(s.cntBuf, wplan)
	s.cntBuf[k]--
	running := math.Max(float64(mplan), 1)
	return w1*wait + w2*running*math.Max(lavg, 1)*(c1-cyc0)*s.kn.AssignDecW
}

// ---------------------------------------------------------------- liveness

func (s *Sched) quiet() bool {
	if s.eBusy || s.xfers > 0 {
		return false
	}
	for _, b := range s.rBusy {
		if b {
			return false
		}
	}
	return true
}

// forceProgress starts any legal task; used only when nothing is in flight, so that
// a policy choice to wait can never leave the system without a future event.
func (s *Sched) forceProgress(t float64) []string {
	var cmds []string
	c := s.tryDPost()
	if c == "" && (len(s.decCont) > 0 || len(s.decNew) > 0) {
		g := append(append([]int(nil), s.decCont...), s.decNew...)
		s.decCont, s.decNew = nil, nil
		s.dcVer++
		s.startWave(g)
		s.eEnd = t + s.S + s.tab.lookup(cDPRE, float64(len(g)))
		c = groupStr("E D PRE -1", g)
	}
	if c == "" {
		c = s.tryPPost()
	}
	if c == "" {
		c = s.tryPPre()
	}
	if c != "" {
		s.eBusy = true
		cmds = append(cmds, c)
	}
	for k := 0; k < s.K; k++ {
		if len(s.procReady[k]) > 0 {
			rid := s.popBest(&s.procReady[k])
			r := &s.reqs[rid]
			full := s.tab.lookup(cPPROC, float64(r.lin))
			ls := r.nextLayer
			s.backlog[k] -= float64(s.NL-ls) / float64(s.NL) * full
			s.rEnd[k] = t + s.S + float64(s.NL-ls)/float64(s.NL)*full
			r.st = stProcRun
			s.rBusy[k] = true
			cmds = append(cmds, procCmd(k, ls, s.NL, rid))
		}
	}
	return cmds
}

// ---------------------------------------------------------------- I/O

func readLine(rd *bufio.Reader) (string, bool) {
	l, err := rd.ReadString('\n')
	if err != nil && len(l) == 0 {
		return "", false
	}
	return strings.TrimSpace(l), true
}

// safeFrame never lets an internal fault crash the process; it answers "0" instead.
func safeFrame(s *Sched, t float64, events [][]string) (resp string) {
	defer func() {
		if recover() != nil {
			resp = "0\n"
		}
	}()
	return s.OnFrame(t, events)
}

func main() {
	rd := bufio.NewReaderSize(os.Stdin, 1<<16)
	wr := bufio.NewWriterSize(os.Stdout, 1<<16)
	var header []string
	for i := 0; i < 3; i++ {
		l, ok := readLine(rd)
		if !ok {
			return
		}
		header = append(header, l)
	}
	n := pi(header[2])
	for i := 0; i < n; i++ {
		l, ok := readLine(rd)
		if !ok {
			return
		}
		header = append(header, l)
	}
	s := NewSched(header, DefaultKnobs())
	var events [][]string
	for {
		l, ok := readLine(rd)
		if !ok {
			return
		}
		if l == "" {
			continue
		}
		if l == "END" {
			return
		}
		t := pf(l)
		l, ok = readLine(rd)
		if !ok {
			return
		}
		e := pi(l)
		events = events[:0]
		for i := 0; i < e; i++ {
			l, ok = readLine(rd)
			if !ok {
				return
			}
			events = append(events, strings.Fields(l))
		}
		if _, err := wr.WriteString(safeFrame(s, t, events)); err != nil {
			return
		}
		if err := wr.Flush(); err != nil {
			return
		}
	}
}
