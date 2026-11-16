// Command load measures a running service: it offers events at a chosen rate
// from many concurrent senders, follows the alarm feed, and reports what was
// accepted, what failed, and how long requests and alarms took.
//
//	go run ./tools/load -target http://localhost:8080 -senders 256 -duration 180s \
//	    -rate 5000 -burst-rate 50000 -burst-start 15s -burst-duration 30s \
//	    -second-burst-start 90s
//
// With -rate 0 every sender sends as fast as the service answers. With a rate,
// events are offered on a fixed schedule whether or not the service keeps up:
// an event that no sender is free to take is counted as not sent, so a slow
// service cannot quietly lower the offered rate. The token queue is bounded
// by -senders. By default its tail drains after the offer window; reports
// distinguish those late request starts from in-flight ACK completions. Use
// -strict-offer-window to expire the tail instead.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"runtime"
	"sync"
	"time"
)

type config struct {
	Target             string        `json:"target"`
	Senders            int           `json:"senders"`
	OfferBuffer        int           `json:"offer_buffer"`
	Devices            int           `json:"devices"`
	Duration           time.Duration `json:"duration_ns"`
	Rate               float64       `json:"rate"`
	BurstRate          float64       `json:"burst_rate"`
	BurstStart         time.Duration `json:"burst_start_ns"`
	BurstDuration      time.Duration `json:"burst_duration_ns"`
	SecondBurstStart   time.Duration `json:"second_burst_start_ns"`
	OfflineStart       time.Duration `json:"offline_start_ns"`
	OfflineDuration    time.Duration `json:"offline_duration_ns"`
	OfflinePercent     int           `json:"offline_percent"`
	OfflineBuffer      int           `json:"offline_buffer_capacity_per_sender"`
	Retries            int           `json:"retries"`
	StrictOfferWindow  bool          `json:"strict_offer_window"`
	RequireFullOffer   bool          `json:"require_full_offer"`
	RequireAllAccepted bool          `json:"require_all_accepted"`
	RequireNoLoss      bool          `json:"require_no_loss"`
	MinAcceptedRate    float64       `json:"min_accepted_rate"`
	MaxRequestP95MS    float64       `json:"max_request_p95_ms"`
	MaxFallP95MS       float64       `json:"max_fall_p95_ms"`
}

func main() {
	var cfg config
	var jsonPath string
	flag.StringVar(&cfg.Target, "target", "http://localhost:8080", "service base URL")
	flag.IntVar(&cfg.Senders, "senders", 64, "concurrent keep-alive senders")
	flag.IntVar(&cfg.OfferBuffer, "offer-buffer", 0, "bounded scheduled arrivals waiting to send; 0 uses the sender count")
	flag.IntVar(&cfg.Devices, "devices", 5000, "simulated devices, 20 per room")
	flag.DurationVar(&cfg.Duration, "duration", 30*time.Second, "how long to offer events")
	flag.Float64Var(&cfg.Rate, "rate", 0, "offered events per second, 0 = as fast as possible")
	flag.Float64Var(&cfg.BurstRate, "burst-rate", 0, "offered events per second during the burst")
	flag.DurationVar(&cfg.BurstStart, "burst-start", 0, "when the burst begins")
	flag.DurationVar(&cfg.BurstDuration, "burst-duration", 0, "how long each burst lasts")
	flag.DurationVar(&cfg.SecondBurstStart, "second-burst-start", 0, "start a second burst at this time (0 disables it)")
	flag.DurationVar(&cfg.OfflineStart, "offline-start", 0, "when selected devices go offline")
	flag.DurationVar(&cfg.OfflineDuration, "offline-duration", 0, "how long selected devices buffer events")
	flag.IntVar(&cfg.OfflinePercent, "offline-percent", 0, "percentage of devices offline during the configured interval")
	flag.IntVar(&cfg.OfflineBuffer, "offline-buffer-capacity", 1000, "maximum buffered events per sender while offline")
	flag.IntVar(&cfg.Retries, "retries", 0, "retries of a 503 or transport error, same event")
	flag.BoolVar(&cfg.StrictOfferWindow, "strict-offer-window", false, "do not start requests after the configured offer duration; count expired arrivals as not sent")
	flag.BoolVar(&cfg.RequireFullOffer, "require-full-offer", false, "validation: fail if paced offers are not all sent")
	flag.BoolVar(&cfg.RequireAllAccepted, "require-all-accepted", false, "validation: require a durable acknowledgement for every sent event")
	flag.BoolVar(&cfg.RequireNoLoss, "require-no-loss", false, "validation: fail if an accepted fall is missing from the feed")
	flag.Float64Var(&cfg.MinAcceptedRate, "min-accepted-rate", 0, "validation: minimum accepted events/s; 0 disables this check")
	flag.Float64Var(&cfg.MaxRequestP95MS, "max-request-p95-ms", 0, "validation: maximum request p95 in milliseconds; 0 disables this check")
	flag.Float64Var(&cfg.MaxFallP95MS, "max-fall-p95-ms", 0, "validation: maximum fall post-to-feed p95 in milliseconds; 0 disables this check")
	flag.StringVar(&jsonPath, "json", "", "also write the report to this file as JSON")
	flag.Parse()
	if cfg.Senders <= 0 || cfg.Devices <= 0 || cfg.Duration <= 0 {
		fatal("-senders, -devices, and -duration must be positive")
	}
	if cfg.OfferBuffer < 0 {
		fatal("-offer-buffer must be non-negative")
	}
	if cfg.OfflineStart < 0 || cfg.OfflineDuration < 0 || cfg.OfflineBuffer < 0 || cfg.OfflinePercent < 0 || cfg.OfflinePercent > 100 {
		fatal("offline start, duration, and buffer capacity must be non-negative and percent must be 0..100")
	}
	if cfg.OfflinePercent > 0 {
		if cfg.Rate <= 0 || cfg.OfflineDuration <= 0 || cfg.OfflineStart+cfg.OfflineDuration >= cfg.Duration {
			fatal("offline mode requires a paced rate, positive duration, and an interval ending before -duration")
		}
	} else if cfg.OfflineDuration != 0 || cfg.OfflineStart != 0 {
		fatal("-offline-start and -offline-duration require -offline-percent")
	}
	if cfg.Retries < 0 {
		fatal("-retries must be non-negative")
	}
	if math.IsNaN(cfg.Rate) || math.IsInf(cfg.Rate, 0) || cfg.Rate < 0 ||
		math.IsNaN(cfg.BurstRate) || math.IsInf(cfg.BurstRate, 0) || cfg.BurstRate < 0 {
		fatal("-rate and -burst-rate must be finite and non-negative")
	}
	if cfg.BurstStart < 0 || cfg.BurstDuration < 0 || cfg.SecondBurstStart < 0 {
		fatal("-burst-start, -burst-duration, and -second-burst-start must be non-negative")
	}
	if cfg.Devices < cfg.Senders {
		cfg.Devices = cfg.Senders // every sender owns at least one device
	}
	if cfg.BurstRate > 0 && cfg.Rate == 0 {
		fatal("-burst-rate needs -rate")
	}
	if err := validateSecondBurst(cfg); err != nil {
		fatal("%v", err)
	}
	if math.IsNaN(cfg.MinAcceptedRate) || math.IsInf(cfg.MinAcceptedRate, 0) || cfg.MinAcceptedRate < 0 ||
		math.IsNaN(cfg.MaxRequestP95MS) || math.IsInf(cfg.MaxRequestP95MS, 0) || cfg.MaxRequestP95MS < 0 ||
		math.IsNaN(cfg.MaxFallP95MS) || math.IsInf(cfg.MaxFallP95MS, 0) || cfg.MaxFallP95MS < 0 {
		fatal("validation thresholds must be finite and non-negative")
	}

	client := newClient(cfg.Senders)
	// The feed consumer has its own connection. Sharing the senders' pool
	// would make it queue behind them for a free connection under load, and
	// the tool would then report its own delay as alarm latency.
	feedClient := newClient(1)
	prefix := fmt.Sprintf("load%d", time.Now().UnixNano())
	falls := newFallTracker()
	cursor, err := currentCursor(feedClient, cfg.Target)
	if err != nil {
		fatal("reading the alarm feed: %v", err)
	}
	stopFeed := make(chan struct{})
	feedDone := make(chan struct{})
	go func() {
		followFeed(feedClient, cfg.Target, cursor, prefix, falls, stopFeed)
		close(feedDone)
	}()

	var sharedDevices *offlineDevices
	if cfg.OfflinePercent > 0 {
		sharedDevices = newOfflineDevices(prefix, cfg.Devices)
	}
	started := time.Now()
	tokens, notSent := offer(cfg, started)
	results := make([]*senderStats, cfg.Senders)
	var wg sync.WaitGroup
	for i := range results {
		results[i] = &senderStats{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := sender{id: i, cfg: cfg, client: client, prefix: prefix, falls: falls, stats: results[i], offlineDevices: sharedDevices}
			s.run(tokens, started)
		}()
	}
	wg.Wait()
	elapsed := time.Since(started)

	// Give alarms still in flight a moment to reach the feed, then stop it.
	falls.waitForDelivery(5 * time.Second)
	close(stopFeed)
	<-feedDone

	report := buildReport(cfg, started, elapsed, results, notSent(), falls)
	report.evaluateValidation()
	report.print(os.Stdout)
	if jsonPath != "" {
		raw, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fatal("encoding the report: %v", err)
		}
		if err := os.WriteFile(jsonPath, raw, 0o644); err != nil {
			fatal("writing the report: %v", err)
		}
	}
	if !report.Reconciles {
		fatal("the counts do not add up")
	}
	if report.ValidationRequested && !report.ValidationPassed {
		fatal("requested validation failed")
	}
}

// offer returns the channel senders take permission from and a function that
// reports how many scheduled events no sender was free to take.
//
// Unpaced, the channel is nil and senders run until the duration ends. Paced,
// a scheduler prefetches at most 1ms of future arrival slots into the bounded
// queue. Senders wait for their logical deadlines before generating events.
// This lets the final slots be ready before the window closes; a full queue
// drops an arrival only once its logical deadline has passed.
func offer(cfg config, started time.Time) (tokens <-chan arrival, notSent func() int64) {
	if cfg.Rate == 0 {
		return nil, func() int64 { return 0 }
	}
	ch := make(chan arrival, offerCapacity(cfg))
	var dropped int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(ch)
		var offered int64
		tick := time.NewTicker(offerLookahead)
		defer tick.Stop()
		for {
			now := time.Now()
			elapsed := min(cfg.Duration, max(time.Duration(0), now.Sub(started)))
			next, missed := dispatchArrivals(cfg, started, now, ch, offered)
			offered = next
			dropped += missed
			if elapsed == cfg.Duration {
				return
			}
			<-tick.C
		}
	}()
	return ch, func() int64 { <-done; return dropped }
}

func offerCapacity(cfg config) int {
	if cfg.OfferBuffer > 0 {
		return cfg.OfferBuffer
	}
	return cfg.Senders
}

// dispatchArrivals never counts a future arrival as dropped merely because
// prefetch filled the queue. It remains pending until a later dispatch.
func dispatchArrivals(cfg config, started, now time.Time, ch chan<- arrival, offered int64) (next, dropped int64) {
	elapsed := min(cfg.Duration, max(time.Duration(0), now.Sub(started)))
	due := prefetched(cfg, elapsed)
	for offered < due {
		a := arrival{at: started.Add(scheduledOffset(cfg, offered)), device: -1}
		if cfg.OfflinePercent > 0 {
			a.device = plannedDevice(offered, cfg.Devices)
		}
		select {
		case ch <- a:
			offered++
		default:
			if a.at.After(now) {
				return offered, dropped
			}
			dropped++
			offered++
		}
	}
	return offered, dropped
}

const offerLookahead = time.Millisecond

// prefetched counts zero-based arrival slots before a bounded future horizon.
// Clipping to the configured total keeps prefetch from adding arrivals.
func prefetched(cfg config, elapsed time.Duration) int64 {
	horizon := min(cfg.Duration, elapsed+offerLookahead)
	return min(int64(math.Ceil(scheduled(cfg, cfg.Duration))), int64(math.Ceil(scheduled(cfg, horizon))))
}

// arrival preserves the logical fixed-rate deadline through the bounded sender
// queue. No event body is built for a scheduled arrival that cannot be sent.
type arrival struct {
	at     time.Time
	device int
}

// scheduledOffset is the deadline of zero-based arrival n. Slots occupy
// [0, duration), so the last arrival is not scheduled exactly at the stop time.
func scheduledOffset(cfg config, n int64) time.Duration {
	var plan [5]phase
	count := fillPhasePlan(cfg, &plan)
	remaining := float64(n)
	for i := 0; i < count; i++ {
		p := plan[i]
		capacity := p.rate * (p.end - p.start).Seconds()
		if remaining < capacity {
			return p.start + time.Duration(remaining/p.rate*float64(time.Second))
		}
		remaining -= capacity
	}
	return cfg.Duration
}

// scheduled integrates baseline and burst rates up to elapsed. The plan is a
// fixed stack array, so the 1ms scheduler loop does not allocate per tick.
func scheduled(cfg config, elapsed time.Duration) float64 {
	elapsed = min(cfg.Duration, max(time.Duration(0), elapsed))
	var plan [5]phase
	count := fillPhasePlan(cfg, &plan)
	var total float64
	for i := 0; i < count; i++ {
		p := plan[i]
		end := min(elapsed, p.end)
		if end > p.start {
			total += p.rate * (end - p.start).Seconds()
		}
	}
	return total
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "load: "+format+"\n", args...)
	os.Exit(1)
}

func environment() map[string]any {
	return map[string]any{
		"go_version": runtime.Version(),
		"num_cpu":    runtime.NumCPU(),
		"gomaxprocs": runtime.GOMAXPROCS(0),
		"goos":       runtime.GOOS,
		"goarch":     runtime.GOARCH,
	}
}
