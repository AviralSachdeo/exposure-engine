package com.github.programmingwithmati.exposureEngine.loadTestExposureEngine;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;

import java.io.IOException;
import java.util.Map;

/**
 * Pushes benchmark results to a Prometheus Pushgateway so they can be
 * scraped by Prometheus and visualised in Grafana.
 *
 * <p>If the Pushgateway is unreachable, push failures are logged to stderr
 * and the benchmark continues normally — metrics are best-effort.</p>
 *
 * <p>Env: {@code PUSHGATEWAY_URL} (default {@code localhost:9091})</p>
 */
public class BenchmarkMetrics {

    private final PushGateway pushGateway;
    private final CollectorRegistry registry;
    private final String benchmarkName;

    private final Gauge latencyP50;
    private final Gauge latencyP90;
    private final Gauge latencyP99;
    private final Gauge latencyP999;
    private final Gauge latencyAvg;
    private final Gauge achievedThroughput;
    private final Gauge targetThroughput;

    public BenchmarkMetrics(String benchmarkName) {
        this.benchmarkName = benchmarkName;
        this.registry = new CollectorRegistry();

        String url = System.getenv("PUSHGATEWAY_URL");
        if (url == null || url.isBlank()) {
            url = "localhost:9091";
        }
        this.pushGateway = new PushGateway(url);

        this.latencyP50 = gauge("benchmark_latency_p50_microseconds", "P50 latency in microseconds");
        this.latencyP90 = gauge("benchmark_latency_p90_microseconds", "P90 latency in microseconds");
        this.latencyP99 = gauge("benchmark_latency_p99_microseconds", "P99 latency in microseconds");
        this.latencyP999 = gauge("benchmark_latency_p999_microseconds", "P99.9 latency in microseconds");
        this.latencyAvg = gauge("benchmark_latency_avg_microseconds", "Average latency in microseconds");
        this.achievedThroughput = gauge("benchmark_achieved_throughput", "Achieved throughput in msgs/sec");
        this.targetThroughput = gauge("benchmark_target_throughput", "Target throughput in msgs/sec");
    }

    /**
     * Push a completed phase's statistics to the Pushgateway.
     *
     * @param mode       e.g. "store_only", "full_pipeline", "kafka_kafka"
     * @param targetRate target msgs/sec for this phase
     * @param stats      array from {@code computeStats}: {p50, p90, p99, p999, avg, achieved}
     */
    public void pushPhaseResult(String mode, int targetRate, double[] stats) {
        String rate = String.valueOf(targetRate);

        latencyP50.labels(mode, rate).set(stats[0]);
        latencyP90.labels(mode, rate).set(stats[1]);
        latencyP99.labels(mode, rate).set(stats[2]);
        latencyP999.labels(mode, rate).set(stats[3]);
        latencyAvg.labels(mode, rate).set(stats[4]);
        achievedThroughput.labels(mode, rate).set(stats[5]);
        targetThroughput.labels(mode, rate).set(targetRate);

        try {
            pushGateway.pushAdd(registry, "benchmark",
                    Map.of("benchmark", benchmarkName));
        } catch (IOException e) {
            System.err.println("[Metrics] Push failed (is Pushgateway running at "
                    + "localhost:9091?): " + e.getMessage());
        }
    }

    private Gauge gauge(String name, String help) {
        return Gauge.build()
                .name(name)
                .help(help)
                .labelNames("mode", "target_rate")
                .register(registry);
    }
}
