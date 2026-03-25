package com.github.programmingwithmati.exposureEngine.loadTestExposureEngine;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.MatchingEngineFill;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateKafkaPayload;
import com.github.programmingwithmati.exposureEngine.store.OffHeapExposureStore;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Arrays;

/**
 * Latency benchmark for the Exposure Engine processing pipeline.
 *
 * Runs at three controlled throughput levels (25k, 50k, 100k msgs/sec) and
 * reports p50, p90, p99, p99.9 and average processing latency for each.
 *
 * Two modes per throughput level:
 *   1. Store-only    — measures {@code OffHeapExposureStore.applyFill()} in isolation
 *   2. Full pipeline — measures JSON deserialization + Double.parseDouble + applyFill
 *
 * No external infrastructure required (no Kafka, no Aeron).
 *
 * Env vars (optional):
 *   LOAD_TEST_DURATION  — seconds per throughput level  (default 10)
 *   LOAD_TEST_WARMUP    — warmup message count          (default 100000)
 */
public class ExposureEngineLatencyBenchmark {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String[] PAIRS = {
            "B-BTC_USDT", "B-ETH_USDT", "B-SOL_USDT", "B-BNB_USDT", "B-XRP_USDT",
            "B-ADA_USDT", "B-DOGE_USDT", "B-AVAX_USDT", "B-DOT_USDT", "B-MATIC_USDT"
    };

    private static final int[] THROUGHPUTS = {25_000, 50_000, 100_000};
    private static final int JSON_POOL_SIZE = 20_000;

    private static PrintStream OUT;

    public static void main(String[] args) throws Exception {
        OUT = System.out;

        int durationSec = intEnv("LOAD_TEST_DURATION", 10);
        int warmup      = intEnv("LOAD_TEST_WARMUP", 100_000);

        printBanner(durationSec, warmup);

        OUT.print("Pre-generating JSON payload pool...");
        String[]  jsonPool    = new String[JSON_POOL_SIZE];
        boolean[] binanceFlag = new boolean[JSON_POOL_SIZE];
        generateJsonPool(jsonPool, binanceFlag);
        OUT.println(" done.\n");

        double[][] storeRes = new double[THROUGHPUTS.length][];
        double[][] pipeRes  = new double[THROUGHPUTS.length][];

        for (int t = 0; t < THROUGHPUTS.length; t++) {
            int target = THROUGHPUTS[t];
            int total  = target * durationSec;

            OUT.printf("════════════════════════════════════════════════════════════════════════%n");
            OUT.printf("  PHASE %d:  %,d msgs/sec  x  %ds  =  %,d messages%n",
                    t + 1, target, durationSec, total);
            OUT.printf("════════════════════════════════════════════════════════════════════════%n");

            OUT.printf("%n  ▶ Store-Only (applyFill latency)%n");
            storeRes[t] = benchStore(target, total, warmup);
            printResult(storeRes[t]);

            OUT.printf("%n  ▶ Full Pipeline (JSON deser + parse + applyFill)%n");
            pipeRes[t] = benchPipeline(target, total, warmup, jsonPool, binanceFlag);
            printResult(pipeRes[t]);

            OUT.println();
        }

        printSummary("STORE-ONLY LATENCY", storeRes);
        printSummary("FULL PIPELINE LATENCY", pipeRes);

        OUT.println("\nBenchmark complete.\n");
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Store-only benchmark
    // ═══════════════════════════════════════════════════════════════════

    private static double[] benchStore(int targetRate, int total, int warmupCount) {
        long baseTs = System.currentTimeMillis();

        suppressStdout();

        OUT.printf("    Warmup (%,d msgs)...", warmupCount);
        OUT.flush();
        OffHeapExposureStore warmupStore = new OffHeapExposureStore();
        for (int i = 0; i < warmupCount; i++) {
            warmupStore.applyFill(
                    PAIRS[i % PAIRS.length], (i & 1) == 0 ? "buy" : "sell",
                    0.1 + (i % 50) * 0.01, 65000.0 + (i % 100) * 10.0,
                    baseTs + i, "W");
        }
        OUT.println(" done.");

        OffHeapExposureStore store = new OffHeapExposureStore();

        OUT.printf("    Running %,d messages at %,d/s...", total, targetRate);
        OUT.flush();

        long[] lat = new long[total];
        long intervalNs = 1_000_000_000L / targetRate;
        long start = System.nanoTime();

        for (int i = 0; i < total; i++) {
            long sched = start + (long) i * intervalNs;
            while (System.nanoTime() < sched) {
                Thread.onSpinWait();
            }

            long t0 = System.nanoTime();
            store.applyFill(
                    PAIRS[i % PAIRS.length], (i & 1) == 0 ? "buy" : "sell",
                    0.1 + (i % 50) * 0.01, 65000.0 + (i % 100) * 10.0,
                    baseTs + i, "B");
            lat[i] = System.nanoTime() - t0;
        }

        long elapsed = System.nanoTime() - start;
        restoreStdout();
        OUT.println(" done.");

        return computeStats(lat, total, elapsed);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Full pipeline benchmark (JSON deser + parse + applyFill)
    // ═══════════════════════════════════════════════════════════════════

    private static double[] benchPipeline(int targetRate, int total, int warmupCount,
                                          String[] jsonPool, boolean[] binanceFlag) {
        suppressStdout();

        OUT.printf("    Warmup (%,d msgs)...", warmupCount);
        OUT.flush();
        OffHeapExposureStore warmupStore = new OffHeapExposureStore();
        for (int i = 0; i < warmupCount; i++) {
            processMessage(warmupStore, jsonPool[i % JSON_POOL_SIZE], binanceFlag[i % JSON_POOL_SIZE]);
        }
        OUT.println(" done.");

        OffHeapExposureStore store = new OffHeapExposureStore();

        OUT.printf("    Running %,d messages at %,d/s...", total, targetRate);
        OUT.flush();

        long[] lat = new long[total];
        long intervalNs = 1_000_000_000L / targetRate;
        long start = System.nanoTime();

        for (int i = 0; i < total; i++) {
            long sched = start + (long) i * intervalNs;
            while (System.nanoTime() < sched) {
                Thread.onSpinWait();
            }

            long t0 = System.nanoTime();
            processMessage(store, jsonPool[i % JSON_POOL_SIZE], binanceFlag[i % JSON_POOL_SIZE]);
            lat[i] = System.nanoTime() - t0;
        }

        long elapsed = System.nanoTime() - start;
        restoreStdout();
        OUT.println(" done.");

        return computeStats(lat, total, elapsed);
    }

    private static void processMessage(OffHeapExposureStore store, String json, boolean binance) {
        try {
            if (binance) {
                OrderUpdateKafkaPayload p = MAPPER.readValue(json, OrderUpdateKafkaPayload.class);
                if (!"FILLED".equals(p.getOrderStatus()) && !"PARTIALLY_FILLED".equals(p.getOrderStatus())) {
                    return;
                }
                store.applyFill(p.getPair(), p.getSide(),
                        Double.parseDouble(p.getQuantity()), Double.parseDouble(p.getPrice()),
                        p.getTimestamp(), "BN");
            } else {
                MatchingEngineFill f = MAPPER.readValue(json, MatchingEngineFill.class);
                store.applyFill(f.getPair(), f.getSide(),
                        Double.parseDouble(f.getQuantity()), Double.parseDouble(f.getPrice()),
                        f.getTimestamp(), "ME");
            }
        } catch (Exception ignored) {
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Statistics
    // ═══════════════════════════════════════════════════════════════════

    /**
     * @return {p50_us, p90_us, p99_us, p999_us, avg_us, achieved_throughput}
     */
    private static double[] computeStats(long[] lat, int n, long elapsedNs) {
        Arrays.sort(lat, 0, n);

        double p50  = lat[(int) (n * 0.50)]                      / 1_000.0;
        double p90  = lat[(int) (n * 0.90)]                      / 1_000.0;
        double p99  = lat[(int) (n * 0.99)]                      / 1_000.0;
        double p999 = lat[Math.min((int) (n * 0.999), n - 1)]    / 1_000.0;

        long sum = 0;
        for (int i = 0; i < n; i++) {
            sum += lat[i];
        }
        double avg = (sum / (double) n) / 1_000.0;

        double throughput = n / (elapsedNs / 1_000_000_000.0);

        return new double[]{p50, p90, p99, p999, avg, throughput};
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Output
    // ═══════════════════════════════════════════════════════════════════

    private static void printBanner(int durationSec, int warmup) {
        OUT.println();
        OUT.println("╔═════════════════════════════════════════════════════════════════════╗");
        OUT.println("║          EXPOSURE ENGINE  —  LATENCY BENCHMARK                     ║");
        OUT.println("╠═════════════════════════════════════════════════════════════════════╣");
        OUT.printf( "║  Duration / phase  : %-3d seconds                                   ║%n", durationSec);
        OUT.printf( "║  Warmup messages   : %,9d                                      ║%n", warmup);
        OUT.printf( "║  Trading pairs     : %-3d                                           ║%n", PAIRS.length);
        OUT.println("║  Throughput targets : 25,000 / 50,000 / 100,000 msgs/sec           ║");
        OUT.println("║  Modes             : Store-Only  +  Full Pipeline                  ║");
        OUT.println("║  Note              : stdout suppressed during timing (no I/O noise)║");
        OUT.println("╚═════════════════════════════════════════════════════════════════════╝");
        OUT.println();
    }

    private static void printResult(double[] s) {
        OUT.printf("    ┌──────────────────────────────────────────────┐%n");
        OUT.printf("    │  Achieved  : %,14.0f msgs/sec        │%n", s[5]);
        OUT.printf("    │  p50       : %14.2f µs              │%n", s[0]);
        OUT.printf("    │  p90       : %14.2f µs              │%n", s[1]);
        OUT.printf("    │  p99       : %14.2f µs              │%n", s[2]);
        OUT.printf("    │  p99.9     : %14.2f µs              │%n", s[3]);
        OUT.printf("    │  Avg       : %14.2f µs              │%n", s[4]);
        OUT.printf("    └──────────────────────────────────────────────┘%n");
    }

    private static void printSummary(String title, double[][] results) {
        OUT.println();
        OUT.printf("  ╔══════════════════════════════════════════════════════════════════════════════════╗%n");
        OUT.printf("  ║  %-80s║%n", title);
        OUT.printf("  ╠════════════════╦══════════╦══════════╦══════════╦══════════╦════════════════════╣%n");
        OUT.printf("  ║ %-14s ║ %8s ║ %8s ║ %8s ║ %8s ║ %18s ║%n",
                "Target", "p50(µs)", "p90(µs)", "p99(µs)", "Avg(µs)", "Achieved");
        OUT.printf("  ╠════════════════╬══════════╬══════════╬══════════╬══════════╬════════════════════╣%n");
        for (int t = 0; t < THROUGHPUTS.length; t++) {
            double[] s = results[t];
            OUT.printf("  ║ %,10d/s   ║ %8.2f ║ %8.2f ║ %8.2f ║ %8.2f ║ %,14.0f/s   ║%n",
                    THROUGHPUTS[t], s[0], s[1], s[2], s[4], s[5]);
        }
        OUT.printf("  ╚════════════════╩══════════╩══════════╩══════════╩══════════╩════════════════════╝%n");
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Helpers
    // ═══════════════════════════════════════════════════════════════════

    private static void generateJsonPool(String[] pool, boolean[] binance) throws JsonProcessingException {
        long baseTs = System.currentTimeMillis();
        for (int i = 0; i < JSON_POOL_SIZE; i++) {
            String pair  = PAIRS[i % PAIRS.length];
            String side  = (i & 1) == 0 ? "buy" : "sell";
            double qty   = 0.1 + (i % 50) * 0.01;
            double price = 65000.0 + (i % 100) * 10.0;

            if (i % 2 == 0) {
                binance[i] = true;
                pool[i] = MAPPER.writeValueAsString(OrderUpdateKafkaPayload.builder()
                        .derivativesFuturesOrderId("LT-BN-" + i)
                        .quantity(String.format("%.4f", qty))
                        .price(String.format("%.2f", price))
                        .pair(pair)
                        .exchangeTradeId(100_000L + i)
                        .ecode("B")
                        .orderStatus("FILLED")
                        .timestamp(baseTs + i)
                        .orderFilledQuantity(String.format("%.4f", qty))
                        .orderAvgPrice(String.format("%.2f", price))
                        .side(side)
                        .isMaker(i % 3 == 0)
                        .build());
            } else {
                binance[i] = false;
                pool[i] = MAPPER.writeValueAsString(MatchingEngineFill.builder()
                        .derivativesFuturesOrderId("LT-ME-" + i)
                        .quantity(String.format("%.4f", qty))
                        .price(String.format("%.2f", price))
                        .pair(pair)
                        .side(side)
                        .exchangeTradeId("LT-TRD-" + i)
                        .timestamp(baseTs + i)
                        .isMaker(i % 3 == 0)
                        .build());
            }
        }
    }

    private static PrintStream savedOut;

    private static void suppressStdout() {
        savedOut = System.out;
        System.setOut(new PrintStream(OutputStream.nullOutputStream()));
    }

    private static void restoreStdout() {
        System.setOut(savedOut);
    }

    private static int intEnv(String key, int def) {
        String v = System.getenv(key);
        if (v != null && !v.isBlank()) {
            try {
                return Integer.parseInt(v);
            } catch (NumberFormatException ignored) {
            }
        }
        return def;
    }
}
