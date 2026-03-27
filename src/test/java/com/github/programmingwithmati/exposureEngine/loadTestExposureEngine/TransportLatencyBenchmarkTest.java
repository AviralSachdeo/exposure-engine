package com.github.programmingwithmati.exposureEngine.loadTestExposureEngine;

import com.github.programmingwithmati.exposureEngine.model.MatchingEngineFill;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateKafkaPayload;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class TransportLatencyBenchmarkTest {

    @Test
    void computeStatsWithZeroElements() throws Exception {
        Method method = TransportLatencyBenchmark.class.getDeclaredMethod(
                "computeStats", long[].class, int.class, long.class);
        method.setAccessible(true);

        long[] latencies = new long[0];
        double[] result = (double[]) method.invoke(null, latencies, 0, 1_000_000_000L);

        assertNotNull(result);
        assertEquals(6, result.length);
        for (double v : result) {
            assertEquals(0.0, v);
        }
    }

    @Test
    void computeStatsWithMultipleElements() throws Exception {
        Method method = TransportLatencyBenchmark.class.getDeclaredMethod(
                "computeStats", long[].class, int.class, long.class);
        method.setAccessible(true);

        long[] latencies = new long[1000];
        for (int i = 0; i < 1000; i++) {
            latencies[i] = (i + 1) * 1000L;
        }
        long elapsed = 1_000_000_000L;

        double[] result = (double[]) method.invoke(null, latencies, 1000, elapsed);

        assertTrue(result[0] > 0, "p50 should be positive");
        assertTrue(result[1] > result[0], "p90 > p50");
        assertTrue(result[2] > result[1], "p99 > p90");
        assertTrue(result[4] > 0, "avg should be positive");
        assertEquals(1000.0, result[5], 10.0);
    }

    @Test
    void computeStatsUniformLatencies() throws Exception {
        Method method = TransportLatencyBenchmark.class.getDeclaredMethod(
                "computeStats", long[].class, int.class, long.class);
        method.setAccessible(true);

        long[] latencies = new long[500];
        Arrays.fill(latencies, 3000L); // 3µs

        double[] result = (double[]) method.invoke(null, latencies, 500, 1_000_000_000L);

        assertEquals(3.0, result[0], 0.01); // p50
        assertEquals(3.0, result[1], 0.01); // p90
        assertEquals(3.0, result[2], 0.01); // p99
        assertEquals(3.0, result[4], 0.01); // avg
    }

    @Test
    void intEnvReturnsDefault() throws Exception {
        Method method = TransportLatencyBenchmark.class.getDeclaredMethod("intEnv", String.class, int.class);
        method.setAccessible(true);
        assertEquals(99, method.invoke(null, "NONEXISTENT_INT_ENV_987", 99));
    }

    @Test
    void strEnvReturnsDefault() throws Exception {
        Method method = TransportLatencyBenchmark.class.getDeclaredMethod("strEnv", String.class, String.class);
        method.setAccessible(true);
        assertEquals("default", method.invoke(null, "NONEXISTENT_STR_ENV_654", "default"));
    }

    @Test
    void strEnvReturnsEnvIfSet() throws Exception {
        Method method = TransportLatencyBenchmark.class.getDeclaredMethod("strEnv", String.class, String.class);
        method.setAccessible(true);
        String result = (String) method.invoke(null, "PATH", "default");
        assertNotEquals("default", result);
    }

    @Test
    void pairsConstant() throws Exception {
        java.lang.reflect.Field field = TransportLatencyBenchmark.class.getDeclaredField("PAIRS");
        field.setAccessible(true);
        String[] pairs = (String[]) field.get(null);
        assertEquals(10, pairs.length);
        assertEquals("B-BTC_USDT", pairs[0]);
        assertEquals("B-MATIC_USDT", pairs[9]);
    }

    @Test
    void throughputsConstant() throws Exception {
        java.lang.reflect.Field field = TransportLatencyBenchmark.class.getDeclaredField("THROUGHPUTS");
        field.setAccessible(true);
        int[] throughputs = (int[]) field.get(null);
        assertArrayEquals(new int[]{25_000, 50_000, 100_000}, throughputs);
    }

    @Test
    void poolSizeConstant() throws Exception {
        java.lang.reflect.Field field = TransportLatencyBenchmark.class.getDeclaredField("POOL_SIZE");
        field.setAccessible(true);
        assertEquals(10_000, field.get(null));
    }

    @Test
    void aeronStreamConstants() throws Exception {
        java.lang.reflect.Field bnField = TransportLatencyBenchmark.class.getDeclaredField("BN_AERON_STREAM");
        bnField.setAccessible(true);
        assertEquals(130, bnField.get(null));

        java.lang.reflect.Field meField = TransportLatencyBenchmark.class.getDeclaredField("ME_AERON_STREAM");
        meField.setAccessible(true);
        assertEquals(120, meField.get(null));
    }

    @Test
    void generatePoolsProducesValidObjects() throws Exception {
        Method method = TransportLatencyBenchmark.class.getDeclaredMethod(
                "generatePools", OrderUpdateKafkaPayload[].class, MatchingEngineFill[].class);
        method.setAccessible(true);

        int poolSize = 10_000; // must match POOL_SIZE
        OrderUpdateKafkaPayload[] bnPool = new OrderUpdateKafkaPayload[poolSize];
        MatchingEngineFill[] mePool = new MatchingEngineFill[poolSize];
        method.invoke(null, bnPool, mePool);

        for (int i = 0; i < 100; i++) {
            assertNotNull(bnPool[i], "BN pool entry " + i + " should not be null");
            assertNotNull(mePool[i], "ME pool entry " + i + " should not be null");

            assertNotNull(bnPool[i].getPair());
            assertNotNull(bnPool[i].getQuantity());
            assertNotNull(bnPool[i].getPrice());
            assertEquals("FILLED", bnPool[i].getOrderStatus());

            assertNotNull(mePool[i].getPair());
            assertNotNull(mePool[i].getQuantity());
            assertNotNull(mePool[i].getPrice());
        }
    }

    @Test
    void generatePoolsAlternatesSides() throws Exception {
        Method method = TransportLatencyBenchmark.class.getDeclaredMethod(
                "generatePools", OrderUpdateKafkaPayload[].class, MatchingEngineFill[].class);
        method.setAccessible(true);

        int poolSize = 10_000; // must match POOL_SIZE
        OrderUpdateKafkaPayload[] bnPool = new OrderUpdateKafkaPayload[poolSize];
        MatchingEngineFill[] mePool = new MatchingEngineFill[poolSize];
        method.invoke(null, bnPool, mePool);

        for (int i = 0; i < 10; i++) {
            String expected = (i % 2 == 0) ? "buy" : "sell";
            assertEquals(expected, bnPool[i].getSide(), "BN entry " + i);
            assertEquals(expected, mePool[i].getSide(), "ME entry " + i);
        }
    }

    @Test
    void generatePoolsCyclesThroughPairs() throws Exception {
        Method method = TransportLatencyBenchmark.class.getDeclaredMethod(
                "generatePools", OrderUpdateKafkaPayload[].class, MatchingEngineFill[].class);
        method.setAccessible(true);

        int poolSize = 10_000; // must match POOL_SIZE
        OrderUpdateKafkaPayload[] bnPool = new OrderUpdateKafkaPayload[poolSize];
        MatchingEngineFill[] mePool = new MatchingEngineFill[poolSize];
        method.invoke(null, bnPool, mePool);

        assertEquals(bnPool[0].getPair(), bnPool[10].getPair());
        assertEquals(mePool[0].getPair(), mePool[10].getPair());
    }

    @Test
    void suppressAndRestoreStdout() throws Exception {
        Method suppress = TransportLatencyBenchmark.class.getDeclaredMethod("suppressStdout");
        suppress.setAccessible(true);
        Method restore = TransportLatencyBenchmark.class.getDeclaredMethod("restoreStdout");
        restore.setAccessible(true);

        java.io.PrintStream original = System.out;
        suppress.invoke(null);
        assertNotEquals(original, System.out);

        restore.invoke(null);
        assertEquals(original, System.out);
    }

    @Test
    void computeStatsThroughputCalculation() throws Exception {
        Method method = TransportLatencyBenchmark.class.getDeclaredMethod(
                "computeStats", long[].class, int.class, long.class);
        method.setAccessible(true);

        long[] latencies = new long[10000];
        for (int i = 0; i < 10000; i++) latencies[i] = 1000L;

        // 10000 messages in 0.5 seconds = 20000 msgs/sec
        double[] result = (double[]) method.invoke(null, latencies, 10000, 500_000_000L);
        assertEquals(20000.0, result[5], 100.0);
    }
}
