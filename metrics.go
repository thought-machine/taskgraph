package taskgraph

import (
	"github.com/prometheus/client_golang/prometheus"
)

// executionLatency records the time taken (in milliseconds) to run a graph.
var executionLatency = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "taskgraph",
		Name:      "execution_latency_millis",
		Help:      "Time taken to run a taskgraph graph in milliseconds",
		Buckets:   []float64{200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200},
	}, []string{"graph", "result"},
)

// RegisterMetrics registers all taskgraph metrics with a prometheus registry.
func RegisterMetrics(registry prometheus.Registerer) {
	registry.MustRegister(
		executionLatency,
	)
}
