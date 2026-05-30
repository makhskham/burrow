// Package metrics exposes Prometheus metrics for a Burrow broker node.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog/log"
)

var (
	MessagesProduced = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "burrow_messages_produced_total",
		Help: "Total messages produced per topic/partition",
	}, []string{"topic", "partition"})

	ReplicationLag = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burrow_replication_lag_offsets",
		Help: "Leader LEO minus follower LEO",
	}, []string{"topic", "partition", "replica"})

	ISRSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burrow_isr_size",
		Help: "Current ISR size per partition",
	}, []string{"topic", "partition"})

	HighWatermark = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burrow_high_watermark",
		Help: "Current high watermark per partition",
	}, []string{"topic", "partition"})

	ProduceLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "burrow_produce_latency_seconds",
		Help:    "End-to-end produce latency by acks mode",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 14),
	}, []string{"acks"})

	LeaderElections = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "burrow_leader_elections_total",
		Help: "Number of leader elections per partition",
	}, []string{"topic", "partition"})

	EpochGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "burrow_epoch",
		Help: "Current epoch per partition",
	}, []string{"topic", "partition"})
)

func init() {
	prometheus.MustRegister(
		MessagesProduced, ReplicationLag, ISRSize, HighWatermark,
		ProduceLatency, LeaderElections, EpochGauge,
	)
}

// ServeHTTP starts the Prometheus /metrics endpoint on addr.
func ServeHTTP(addr string) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("ok"))
	})
	log.Info().Str("addr", addr).Msg("metrics server listening")
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal().Err(err).Msg("metrics server failed")
	}
}
