// Command load measures a running service: it offers events at a chosen rate
// from many concurrent senders, follows the alarm feed, and reports what was
// accepted, what failed, and how long requests and alarms took.
//
//	go run ./tools/load -target http://localhost:8080 -senders 256 -duration 60s \
//	    -rate 5000 -burst-rate 50000 -burst-start 20s -burst-duration 30s
//
// With -rate 0 every sender sends as fast as the service answers. With a rate,
// events are offered on a fixed schedule whether or not the service keeps up:
// an event that no sender is free to take is counted as not sent, so a slow
// service cannot quietly lower the offered rate.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"
)

type config struct {
	Target        string        `json:"target"`
	Senders       int           `json:"senders"`
	Devices       int           `json:"devices"`
	Duration      time.Duration `json:"duration_ns"`
	Rate          float64       `json:"rate"`
	BurstRate     float64       `json:"burst_rate"`
	BurstStart    time.Duration `json:"burst_start_ns"`
	BurstDuration time.Duration `json:"burst_duration_ns"`
	Retries       int           `json:"retries"`
}

func main() {
	var cfg config
	var jsonPath string
	flag.StringVar(&cfg.Target, "target", "http://localhost:8080", "service base URL")
	flag.IntVar(&cfg.Senders, "senders", 64, "concurrent keep-alive senders")
	flag.IntVar(&cfg.Devices, "devices", 5000, "simulated devices, 20 per room")
	flag.DurationVar(&cfg.Duration, "duration", 30*time.Second, "how long to offer events")
	flag.Float64Var(&cfg.Rate, "rate", 0, "offered events per second, 0 = as fast as possible")
	flag.Float64Var(&cfg.BurstRate, "burst-rate", 0, "offered events per second during the burst")
	flag.DurationVar(&cfg.BurstStart, "burst-start", 0, "when the burst begins")
	flag.DurationVar(&cfg.BurstDuration, "burst-duration", 0, "how long the burst lasts")
	flag.IntVar(&cfg.Retries, "retries", 0, "retries of a 503 or transport error, same event")
	flag.StringVar(&jsonPath, "json", "", "also write the report to this file as JSON")
	flag.Parse()
	if cfg.Devices < cfg.Senders {
		cfg.Devices = cfg.Senders // every sender owns at least one device
	}
	if cfg.BurstRate > 0 && cfg.Rate == 0 {
		fatal("-burst-rate needs -rate")
	}

	client := newClient(cfg.Senders)
	prefix := fmt.Sprintf("load%d", time.Now().UnixNano())
	falls := newFallTracker()
	cursor, err := currentCursor(client, cfg.Target)
	if err != nil {
		fatal("reading the alarm feed: %v", err)
	}
	stopFeed := make(chan struct{})
	feedDone := make(chan struct{})
	go func() {
		followFeed(client, cfg.Target, cursor, prefix, falls, stopFeed)
		close(feedDone)
	}()

	started := time.Now()
	tokens, notSent := offer(cfg, started)
	results := make([]*senderStats, cfg.Senders)
	var wg sync.WaitGroup
	for i := range results {
		results[i] = &senderStats{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := sender{id: i, cfg: cfg, client: client, prefix: prefix, falls: falls, stats: results[i]}
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
}

// offer returns the channel senders take permission from and a function that
// reports how many scheduled events no sender was free to take.
//
// Unpaced, the channel is nil and senders run until the duration ends. Paced,
// a scheduler works out how many events should have been offered by now and
// hands out that many tokens without ever waiting for a sender.
func offer(cfg config, started time.Time) (tokens <-chan struct{}, notSent func() int64) {
	if cfg.Rate == 0 {
		return nil, func() int64 { return 0 }
	}
	ch := make(chan struct{}, cfg.Senders)
	var dropped int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer close(ch)
		var offered int64
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for now := range tick.C {
			elapsed := now.Sub(started)
			if elapsed > cfg.Duration {
				elapsed = cfg.Duration
			}
			due := int64(scheduled(cfg, elapsed))
			for ; offered < due; offered++ {
				select {
				case ch <- struct{}{}:
				default:
					dropped++
				}
			}
			if elapsed == cfg.Duration {
				return
			}
		}
	}()
	return ch, func() int64 { <-done; return dropped }
}

// scheduled is how many events should have been offered after elapsed time.
func scheduled(cfg config, elapsed time.Duration) float64 {
	total := cfg.Rate * elapsed.Seconds()
	if cfg.BurstRate == 0 || elapsed <= cfg.BurstStart {
		return total
	}
	inBurst := elapsed - cfg.BurstStart
	if inBurst > cfg.BurstDuration {
		inBurst = cfg.BurstDuration
	}
	return total + (cfg.BurstRate-cfg.Rate)*inBurst.Seconds()
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
