package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gitlab.com/earthscope/gnsstools/codecs/rinex"
	"gitlab.com/earthscope/gnsstools/core/gnss/observation"
)

const (
	CLIGHT = 299792458.0
	F_L1   = 1575.42e6
	F_L2   = 1227.60e6

	LAM_L1 = CLIGHT / F_L1
	LAM_L2 = CLIGHT / F_L2

	GAMMA12      = (F_L1 / F_L2) * (F_L1 / F_L2)
	MP1Coeff     = 2.0 / (GAMMA12 - 1.0)
	MP2Coeff     = MP1Coeff * GAMMA12
	IONCoeff21   = (F_L1 * F_L1) / ((F_L1 * F_L1) - (F_L2 * F_L2))
	IONJumpThres = 400.0
	GapMinSec    = 10.0 * 60.0
)

type Header struct {
	FileName       string
	Version        string
	Receiver       string
	Antenna        string
	ApproxPosition [3]float64
	IntervalSec    float64
	ObsTypes       []string
}

type ObsRecord struct {
	Epoch  time.Time
	SV     string
	Values map[string]float64
}

type RinexObs struct {
	Header  Header
	Records []ObsRecord
}

type svStats struct {
	Mean float64
	Std  float64
	Min  float64
	Max  float64
	N    int
}

type mpStats struct {
	MP1RMS *float64
	MP2RMS *float64
	NObs   int
}

type gapEntry struct {
	Start       time.Time
	End         time.Time
	DurationSec float64
}

type epochSummary struct {
	NEpochs        int
	FirstEpoch     time.Time
	LastEpoch      time.Time
	TimeSpanHours  float64
	IntervalSec    float64
	ExpectedEpochs int
	SVsMean        float64
	SVsMin         int
	SVsMax         int
}

type timeVal struct {
	T time.Time
	V float64
}

func parseRinexObs(path string) (*RinexObs, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	scanner, err := rinex.NewScanner(f)
	if err != nil {
		return nil, fmt.Errorf("failed to create RINEX scanner: %w", err)
	}

	h := Header{FileName: filepath.Base(path), IntervalSec: 30.0}

	// Extract header info
	if ver, err := scanner.Header.GetRinexVersion(); err == nil {
		h.Version = ver.String()
	}
	if rec, found := scanner.Header.FindRecord("REC # / TYPE / VERS"); found {
		h.Receiver = strings.TrimSpace(rec.Content)
	}
	if ant, found := scanner.Header.FindRecord("ANT # / TYPE"); found {
		h.Antenna = strings.TrimSpace(ant.Content)
	}
	if pos, err := scanner.Header.GetApproxPosition(); err == nil {
		h.ApproxPosition = [3]float64{pos[0], pos[1], pos[2]}
	}

	// Read all observation epochs
	var records []ObsRecord
	for {
		epoch, err := scanner.NextEpoch()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("error reading epoch: %w", err)
		}
		if epoch.EpochFlag != observation.Ok && epoch.EpochFlag != observation.PowerFailure {
			continue
		}
		for _, sat := range epoch.Satellites {
			sv := sat.SatelliteKey.String()
			vals := map[string]float64{}
			for _, obs := range sat.Observations {
				freq := obs.Code.Frequency
				// Map frequency bands to RINEX 2-style obs codes
				bandSuffix := ""
				switch freq {
				case observation.GPS_L1, observation.Galileo_E1, observation.SBAS_L1, observation.QZSS_L1:
					bandSuffix = "1"
				case observation.GPS_L2, observation.QZSS_L2:
					bandSuffix = "2"
				case observation.GPS_L5, observation.Galileo_E5a, observation.SBAS_L5, observation.QZSS_L5:
					bandSuffix = "5"
				case observation.GLONASS_G1:
					bandSuffix = "1"
				case observation.GLONASS_G2:
					bandSuffix = "2"
				default:
					continue
				}
				if obs.Phase != 0 {
					vals["L"+bandSuffix] = obs.Phase
				}
				if obs.Pseudorange != 0 {
					// Use C1/P1 convention for band 1, C2/P2 for band 2
					if bandSuffix == "1" {
						vals["C1"] = obs.Pseudorange
					} else if bandSuffix == "2" {
						vals["P2"] = obs.Pseudorange
					}
				}
				if obs.SNR != 0 {
					vals["S"+bandSuffix] = obs.SNR
				}
				if obs.Doppler != 0 {
					vals["D"+bandSuffix] = obs.Doppler
				}
			}
			records = append(records, ObsRecord{Epoch: epoch.Time, SV: sv, Values: vals})
		}
	}

	// Estimate interval from data if we have enough epochs
	if len(records) > 1 {
		epochTimes := map[time.Time]bool{}
		for _, r := range records {
			epochTimes[r.Epoch] = true
		}
		times := make([]time.Time, 0, len(epochTimes))
		for t := range epochTimes {
			times = append(times, t)
		}
		sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
		if len(times) > 1 {
			h.IntervalSec = times[1].Sub(times[0]).Seconds()
		}
	}

	return &RinexObs{Header: h, Records: records}, nil
}

func groupBySV(records []ObsRecord) map[string][]ObsRecord {
	out := map[string][]ObsRecord{}
	for _, r := range records {
		out[r.SV] = append(out[r.SV], r)
	}
	for sv := range out {
		sort.Slice(out[sv], func(i, j int) bool {
			return out[sv][i].Epoch.Before(out[sv][j].Epoch)
		})
	}
	return out
}

func epochSummaryCalc(records []ObsRecord, intervalSec float64) (epochSummary, map[time.Time]int) {
	epochCounts := map[time.Time]int{}
	for _, r := range records {
		epochCounts[r.Epoch]++
	}
	epochs := make([]time.Time, 0, len(epochCounts))
	for t := range epochCounts {
		epochs = append(epochs, t)
	}
	sort.Slice(epochs, func(i, j int) bool { return epochs[i].Before(epochs[j]) })

	if len(epochs) == 0 {
		return epochSummary{}, epochCounts
	}

	first := epochs[0]
	last := epochs[len(epochs)-1]
	spanHours := last.Sub(first).Seconds() / 3600.0
	expected := int(last.Sub(first).Seconds()/intervalSec) + 1

	minSV := math.MaxInt32
	maxSV := 0
	sum := 0.0
	for _, t := range epochs {
		n := epochCounts[t]
		if n < minSV {
			minSV = n
		}
		if n > maxSV {
			maxSV = n
		}
		sum += float64(n)
	}

	return epochSummary{
		NEpochs:        len(epochs),
		FirstEpoch:     first,
		LastEpoch:      last,
		TimeSpanHours:  spanHours,
		IntervalSec:    intervalSec,
		ExpectedEpochs: expected,
		SVsMean:        sum / float64(len(epochs)),
		SVsMin:         minSV,
		SVsMax:         maxSV,
	}, epochCounts
}

func computeMultipath(records []ObsRecord, intervalSec float64) map[string]mpStats {
	bySV := groupBySV(records)
	res := map[string]mpStats{}
	gapThreshold := math.Max(intervalSec*2.0, 900.0)

	for sv, arr := range bySV {
		var mp1Series []timeVal
		var mp2Series []timeVal
		for _, r := range arr {
			l1, okL1 := r.Values["L1"]
			l2, okL2 := r.Values["L2"]
			if !(okL1 && okL2) {
				continue
			}
			if p1, ok := r.Values["P1"]; ok {
				v := p1 - (1.0+MP1Coeff)*LAM_L1*l1 + MP1Coeff*LAM_L2*l2
				mp1Series = append(mp1Series, timeVal{T: r.Epoch, V: v})
			} else if c1, ok := r.Values["C1"]; ok {
				v := c1 - (1.0+MP1Coeff)*LAM_L1*l1 + MP1Coeff*LAM_L2*l2
				mp1Series = append(mp1Series, timeVal{T: r.Epoch, V: v})
			}

			if p2, ok := r.Values["P2"]; ok {
				v := p2 - MP2Coeff*LAM_L1*l1 + (MP2Coeff-1.0)*LAM_L2*l2
				mp2Series = append(mp2Series, timeVal{T: r.Epoch, V: v})
			} else if c2, ok := r.Values["C2"]; ok {
				v := c2 - MP2Coeff*LAM_L1*l1 + (MP2Coeff-1.0)*LAM_L2*l2
				mp2Series = append(mp2Series, timeVal{T: r.Epoch, V: v})
			}
		}

		arcDetrend := func(series []timeVal) []float64 {
			if len(series) == 0 {
				return nil
			}
			corr := make([]float64, len(series))
			start := 0
			for i := 1; i <= len(series); i++ {
				boundary := i == len(series)
				if !boundary {
					dt := series[i].T.Sub(series[i-1].T).Seconds()
					if dt > gapThreshold {
						boundary = true
					}
				}
				if boundary {
					sum, n := 0.0, 0
					for k := start; k < i; k++ {
						sum += series[k].V
						n++
					}
					mean := 0.0
					if n > 0 {
						mean = sum / float64(n)
					}
					for k := start; k < i; k++ {
						corr[k] = series[k].V - mean
					}
					start = i
				}
			}
			return corr
		}

		calcRMS := func(v []float64) *float64 {
			if len(v) == 0 {
				return nil
			}
			s := 0.0
			for _, x := range v {
				s += x * x
			}
			r := math.Sqrt(s / float64(len(v)))
			return &r
		}

		mp1Corr := arcDetrend(mp1Series)
		mp2Corr := arcDetrend(mp2Series)
		res[sv] = mpStats{
			MP1RMS: calcRMS(mp1Corr),
			MP2RMS: calcRMS(mp2Corr),
			NObs:   maxInt(len(mp1Corr), len(mp2Corr)),
		}
	}
	return res
}

func detectIONSlips(records []ObsRecord) (int, int) {
	bySV := groupBySV(records)
	slipCount := 0
	nObs := 0
	for _, arr := range bySV {
		first := true
		lastIon := 0.0
		for _, r := range arr {
			l1, okL1 := r.Values["L1"]
			l2, okL2 := r.Values["L2"]
			if !(okL1 && okL2) {
				continue
			}
			ion := IONCoeff21 * (LAM_L2*l2 - LAM_L1*l1)
			nObs++
			if !first {
				if math.Abs(ion-lastIon) > IONJumpThres {
					slipCount++
				}
			}
			lastIon = ion
			first = false
		}
	}
	return slipCount, nObs
}

func computeSNRStats(records []ObsRecord) map[string]map[string]svStats {
	bands := []string{"S1", "S2", "S5"}
	out := map[string]map[string]svStats{}
	for _, b := range bands {
		bySVVals := map[string][]float64{}
		for _, r := range records {
			if v, ok := r.Values[b]; ok {
				bySVVals[r.SV] = append(bySVVals[r.SV], v)
			}
		}
		if len(bySVVals) == 0 {
			continue
		}
		out[b] = map[string]svStats{}
		for sv, vals := range bySVVals {
			if len(vals) == 0 {
				continue
			}
			sum := 0.0
			mn := vals[0]
			mx := vals[0]
			for _, v := range vals {
				sum += v
				if v < mn {
					mn = v
				}
				if v > mx {
					mx = v
				}
			}
			mean := sum / float64(len(vals))
			varsq := 0.0
			for _, v := range vals {
				d := v - mean
				varsq += d * d
			}
			std := math.Sqrt(varsq / float64(len(vals)))
			out[b][sv] = svStats{Mean: mean, Std: std, Min: mn, Max: mx, N: len(vals)}
		}
	}
	return out
}

func detectGaps(records []ObsRecord, gapMinSec float64) map[string][]gapEntry {
	bySV := groupBySV(records)
	out := map[string][]gapEntry{}
	for sv, arr := range bySV {
		for i := 1; i < len(arr); i++ {
			dt := arr[i].Epoch.Sub(arr[i-1].Epoch).Seconds()
			if dt > gapMinSec {
				out[sv] = append(out[sv], gapEntry{
					Start:       arr[i-1].Epoch,
					End:         arr[i].Epoch,
					DurationSec: dt,
				})
			}
		}
	}
	return out
}

func sortedKeys[K ~string, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func formatReport(h Header, es epochSummary, mp map[string]mpStats, snr map[string]map[string]svStats, ionObs, slipCount int, gaps map[string][]gapEntry) string {
	var b strings.Builder
	w := func(s string, a ...any) { fmt.Fprintf(&b, s, a...) }

	w("========================================================================\n")
	w("  GNSS RINEX Observation Quality Check Report (teqc-style, Go)\n")
	w("========================================================================\n\n")

	w("  Filename:          %s\n", h.FileName)
	w("  RINEX version:     %s\n", h.Version)
	w("  Receiver:          %s\n", h.Receiver)
	w("  Antenna:           %s\n", h.Antenna)
	w("  Approx position:   X=%.4f  Y=%.4f  Z=%.4f\n\n", h.ApproxPosition[0], h.ApproxPosition[1], h.ApproxPosition[2])

	w("  Time Summary\n")
	w("  ----------------------------------------\n")
	w("  First epoch:       %s\n", es.FirstEpoch.Format("2006-01-02 15:04:05"))
	w("  Last epoch:        %s\n", es.LastEpoch.Format("2006-01-02 15:04:05"))
	w("  Time span:         %.2f hours\n", es.TimeSpanHours)
	w("  Interval:          %.0f seconds\n", es.IntervalSec)
	w("  Epochs observed:   %d\n", es.NEpochs)
	w("  Epochs expected:   %d\n", es.ExpectedEpochs)
	comp := 0.0
	if es.ExpectedEpochs > 0 {
		comp = float64(es.NEpochs) / float64(es.ExpectedEpochs) * 100.0
	}
	w("  Completeness:      %.1f%%\n\n", comp)

	w("  Satellite Summary\n")
	w("  ----------------------------------------\n")
	w("  Mean SVs/epoch:    %.1f\n", es.SVsMean)
	w("  Min SVs/epoch:     %d\n", es.SVsMin)
	w("  Max SVs/epoch:     %d\n\n", es.SVsMax)

	if len(mp) > 0 {
		w("  Multipath (code-minus-carrier)\n")
		w("  ------------------------------------------------------------\n")
		w("  %-8s %12s %12s %8s\n", "SV", "MP1 RMS (m)", "MP2 RMS (m)", "#obs")
		w("  ------------------------------------------------------------\n")
		keys := sortedKeys(mp)
		all1 := []float64{}
		all2 := []float64{}
		totalObs := 0
		for _, sv := range keys {
			s := mp[sv]
			mp1 := "n/a"
			mp2 := "n/a"
			if s.MP1RMS != nil {
				mp1 = fmt.Sprintf("%.4f", *s.MP1RMS)
				all1 = append(all1, *s.MP1RMS)
			}
			if s.MP2RMS != nil {
				mp2 = fmt.Sprintf("%.4f", *s.MP2RMS)
				all2 = append(all2, *s.MP2RMS)
			}
			totalObs += s.NObs
			w("  %-8s %12s %12s %8d\n", sv, mp1, mp2, s.NObs)
		}
		w("  ------------------------------------------------------------\n")
		if len(all1) > 0 && len(all2) > 0 {
			w("  %-8s %12.4f %12.4f %8d\n", "Overall", mean(all1), mean(all2), totalObs)
		}
		w("\n")
	}

	w("  Ionospheric / Slip Summary\n")
	w("  ----------------------------------------\n")
	w("  ION slips detected: %d\n", slipCount)
	w("  Observations:       %d\n", ionObs)
	if ionObs > 0 {
		ratio := float64(ionObs) / float64(maxInt(slipCount, 1))
		w("  Obs/slip ratio:     %.0f\n", ratio)
	}
	w("\n")

	for _, band := range []string{"S1", "S2", "S5"} {
		svMap, ok := snr[band]
		if !ok {
			continue
		}
		label := map[string]string{"S1": "L1", "S2": "L2", "S5": "L5"}[band]
		w("  Signal-to-Noise Ratio (%s)\n", label)
		w("  -------------------------------------------------------\n")
		w("  %-8s %8s %8s %8s %8s %8s\n", "SV", "Mean", "Std", "Min", "Max", "#obs")
		w("  -------------------------------------------------------\n")
		keys := sortedKeys(svMap)
		means := []float64{}
		for _, sv := range keys {
			s := svMap[sv]
			means = append(means, s.Mean)
			w("  %-8s %8.1f %8.1f %8.1f %8.1f %8d\n", sv, s.Mean, s.Std, s.Min, s.Max, s.N)
		}
		w("  -------------------------------------------------------\n")
		if len(means) > 0 {
			w("  %-8s %8.1f\n", "Overall", mean(means))
		}
		w("\n")
	}

	if len(gaps) > 0 {
		total := 0
		for _, g := range gaps {
			total += len(g)
		}
		w("  Data Gaps (>10 min)\n")
		w("  ------------------------------------------------------------\n")
		w("  Total gaps:  %d\n", total)
		keys := sortedKeys(gaps)
		for _, sv := range keys {
			for _, g := range gaps[sv] {
				w("    %s  %s → %s  (%.1f min)\n",
					sv,
					g.Start.Format("2006-01-02 15:04:05"),
					g.End.Format("2006-01-02 15:04:05"),
					g.DurationSec/60.0)
			}
		}
		w("\n")
	}

	w("========================================================================\n")
	return b.String()
}

func mean(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := 0.0
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <obs_file> [nav_file]\n", filepath.Base(os.Args[0]))
		fmt.Fprintln(os.Stderr, "Example:")
		fmt.Fprintf(os.Stderr, "  %s data/rinex/onof.03o data/rinex/onof.03n\n", filepath.Base(os.Args[0]))
	}
	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}
	obsPath := flag.Arg(0)
	if _, err := os.Stat(obsPath); err != nil {
		fmt.Fprintf(os.Stderr, "error: observation file not found: %s\n", obsPath)
		os.Exit(1)
	}

	fmt.Printf("Reading %s...\n", obsPath)
	obs, err := parseRinexObs(obsPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error parsing RINEX OBS: %v\n", err)
		os.Exit(1)
	}

	if flag.NArg() >= 2 {
		navPath := flag.Arg(1)
		if _, err := os.Stat(navPath); err == nil {
			fmt.Printf("Navigation file provided: %s (currently not required for this QC output)\n", navPath)
		}
	}

	es, _ := epochSummaryCalc(obs.Records, obs.Header.IntervalSec)
	mp := computeMultipath(obs.Records, obs.Header.IntervalSec)
	slipCount, ionObs := detectIONSlips(obs.Records)
	snr := computeSNRStats(obs.Records)
	gaps := detectGaps(obs.Records, GapMinSec)

	report := formatReport(obs.Header, es, mp, snr, ionObs, slipCount, gaps)
	fmt.Print(report)

	reportPath := obsPath + ".S"
	if err := os.WriteFile(reportPath, []byte(report), 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "error writing report: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("\nReport saved to: %s\n", reportPath)
}
