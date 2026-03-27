package com.github.programmingwithmati.exposureEngine.loadTestExposureEngine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.MatchingEngineFill;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateKafkaPayload;
import com.github.programmingwithmati.exposureEngine.store.OffHeapExposureStore;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class ExposureEngineLatencyBenchmarkTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void computeStatsReturnsCorrectPercentiles() throws Exception {
        Method method = ExposureEngineLatencyBenchmark.class.getDeclaredMethod(
                "computeStats", long[].class, int.class, long.class);
        method.setAccessible(true);

        long[] latencies = new long[100];
        for (int i = 0; i < 100; i++) {
            latencies[i] = (i + 1) * 1000L; // 1µs to 100µs in nanoseconds
        }
        long elapsed = 1_000_000_000L; // 1 second

        double[] result = (double[]) method.invoke(null, latencies, 100, elapsed);

        assertNotNull(result);
        assertEquals(6, result.length);

        // p50 ~ 50µs
        assertTrue(result[0] > 0, "p50 should be positive");
        // p90 ~ 90µs
        assertTrue(result[1] > result[0], "p90 > p50");
        // p99 ~ 99µs
        assertTrue(result[2] > result[1], "p99 > p90");
        // p999 ~ 100µs
        assertTrue(result[3] >= result[2], "p999 >= p99");
        // avg
        assertTrue(result[4] > 0, "avg should be positive");
        // throughput ~ 100 msgs/sec
        assertEquals(100.0, result[5], 1.0);
    }

    @Test
    void computeStatsWithSingleElement() throws Exception {
        Method method = ExposureEngineLatencyBenchmark.class.getDeclaredMethod(
                "computeStats", long[].class, int.class, long.class);
        method.setAccessible(true);

        long[] latencies = {5000L};
        double[] result = (double[]) method.invoke(null, latencies, 1, 1_000_000_000L);

        assertNotNull(result);
        assertEquals(6, result.length);
        assertEquals(5.0, result[4], 0.1); // avg = 5µs
    }

    @Test
    void computeStatsWithUniformLatencies() throws Exception {
        Method method = ExposureEngineLatencyBenchmark.class.getDeclaredMethod(
                "computeStats", long[].class, int.class, long.class);
        method.setAccessible(true);

        long[] latencies = new long[1000];
        Arrays.fill(latencies, 2000L); // all 2µs

        double[] result = (double[]) method.invoke(null, latencies, 1000, 1_000_000_000L);

        assertEquals(2.0, result[0], 0.01); // p50
        assertEquals(2.0, result[1], 0.01); // p90
        assertEquals(2.0, result[2], 0.01); // p99
        assertEquals(2.0, result[4], 0.01); // avg
    }

    @Test
    void processMessageBinanceFilled() throws Exception {
        Method method = ExposureEngineLatencyBenchmark.class.getDeclaredMethod(
                "processMessage", OffHeapExposureStore.class, String.class, boolean.class);
        method.setAccessible(true);

        OffHeapExposureStore store = new OffHeapExposureStore();

        String json = MAPPER.writeValueAsString(OrderUpdateKafkaPayload.builder()
                .derivativesFuturesOrderId("LT-1")
                .quantity("1.5")
                .price("65000.00")
                .pair("B-BTC_USDT")
                .exchangeTradeId(1L)
                .ecode("B")
                .orderStatus("FILLED")
                .timestamp(1000L)
                .orderFilledQuantity("1.5")
                .orderAvgPrice("65000.00")
                .side("buy")
                .isMaker(false)
                .build());

        method.invoke(null, store, json, true);

        assertEquals(1.5, store.getNetQuantity("B-BTC_USDT"), 1e-9);
    }

    @Test
    void processMessageBinanceNotFilled() throws Exception {
        Method method = ExposureEngineLatencyBenchmark.class.getDeclaredMethod(
                "processMessage", OffHeapExposureStore.class, String.class, boolean.class);
        method.setAccessible(true);

        OffHeapExposureStore store = new OffHeapExposureStore();

        String json = MAPPER.writeValueAsString(OrderUpdateKafkaPayload.builder()
                .orderStatus("NEW")
                .quantity("1.0")
                .price("65000.00")
                .pair("B-BTC_USDT")
                .side("buy")
                .timestamp(1000L)
                .build());

        method.invoke(null, store, json, true);

        assertEquals(0.0, store.getNetQuantity("B-BTC_USDT"));
    }

    @Test
    void processMessageMEFill() throws Exception {
        Method method = ExposureEngineLatencyBenchmark.class.getDeclaredMethod(
                "processMessage", OffHeapExposureStore.class, String.class, boolean.class);
        method.setAccessible(true);

        OffHeapExposureStore store = new OffHeapExposureStore();

        String json = MAPPER.writeValueAsString(MatchingEngineFill.builder()
                .derivativesFuturesOrderId("ME-1")
                .quantity("3.0")
                .price("3400.00")
                .pair("B-ETH_USDT")
                .side("sell")
                .exchangeTradeId("TRD-1")
                .timestamp(2000L)
                .isMaker(false)
                .build());

        method.invoke(null, store, json, false);

        assertEquals(-3.0, store.getNetQuantity("B-ETH_USDT"), 1e-9);
    }

    @Test
    void processMessageInvalidJson() throws Exception {
        Method method = ExposureEngineLatencyBenchmark.class.getDeclaredMethod(
                "processMessage", OffHeapExposureStore.class, String.class, boolean.class);
        method.setAccessible(true);

        OffHeapExposureStore store = new OffHeapExposureStore();
        assertDoesNotThrow(() -> method.invoke(null, store, "invalid", true));
        assertDoesNotThrow(() -> method.invoke(null, store, "invalid", false));
    }

    @Test
    void intEnvReturnsDefault() throws Exception {
        Method method = ExposureEngineLatencyBenchmark.class.getDeclaredMethod(
                "intEnv", String.class, int.class);
        method.setAccessible(true);

        assertEquals(42, method.invoke(null, "NONEXISTENT_INT_ENV_VAR", 42));
    }

    @Test
    void pairsConstant() throws Exception {
        java.lang.reflect.Field field = ExposureEngineLatencyBenchmark.class.getDeclaredField("PAIRS");
        field.setAccessible(true);
        String[] pairs = (String[]) field.get(null);
        assertEquals(10, pairs.length);
        assertEquals("B-BTC_USDT", pairs[0]);
    }

    @Test
    void throughputsConstant() throws Exception {
        java.lang.reflect.Field field = ExposureEngineLatencyBenchmark.class.getDeclaredField("THROUGHPUTS");
        field.setAccessible(true);
        int[] throughputs = (int[]) field.get(null);
        assertEquals(3, throughputs.length);
        assertEquals(25_000, throughputs[0]);
        assertEquals(50_000, throughputs[1]);
        assertEquals(100_000, throughputs[2]);
    }

    @Test
    void jsonPoolSizeConstant() throws Exception {
        java.lang.reflect.Field field = ExposureEngineLatencyBenchmark.class.getDeclaredField("JSON_POOL_SIZE");
        field.setAccessible(true);
        assertEquals(20_000, field.get(null));
    }

    @Test
    void generateJsonPoolProducesValidJson() throws Exception {
        Method method = ExposureEngineLatencyBenchmark.class.getDeclaredMethod(
                "generateJsonPool", String[].class, boolean[].class);
        method.setAccessible(true);

        int poolSize = 20_000; // must match JSON_POOL_SIZE
        String[] pool = new String[poolSize];
        boolean[] binance = new boolean[poolSize];
        method.invoke(null, pool, binance);

        for (int i = 0; i < 100; i++) {
            assertNotNull(pool[i], "Pool entry " + i + " should not be null");
            assertFalse(pool[i].isEmpty(), "Pool entry " + i + " should not be empty");

            if (binance[i]) {
                OrderUpdateKafkaPayload payload = MAPPER.readValue(pool[i], OrderUpdateKafkaPayload.class);
                assertNotNull(payload.getPair());
                assertNotNull(payload.getQuantity());
                assertNotNull(payload.getPrice());
            } else {
                MatchingEngineFill fill = MAPPER.readValue(pool[i], MatchingEngineFill.class);
                assertNotNull(fill.getPair());
                assertNotNull(fill.getQuantity());
                assertNotNull(fill.getPrice());
            }
        }
    }

    @Test
    void generateJsonPoolAlternatesBinanceAndME() throws Exception {
        Method method = ExposureEngineLatencyBenchmark.class.getDeclaredMethod(
                "generateJsonPool", String[].class, boolean[].class);
        method.setAccessible(true);

        int poolSize = 20_000; // must match JSON_POOL_SIZE
        String[] pool = new String[poolSize];
        boolean[] binance = new boolean[poolSize];
        method.invoke(null, pool, binance);

        for (int i = 0; i < 10; i++) {
            assertEquals(i % 2 == 0, binance[i],
                    "Entry " + i + " should be " + (i % 2 == 0 ? "Binance" : "ME"));
        }
    }

    @Test
    void suppressAndRestoreStdout() throws Exception {
        Method suppress = ExposureEngineLatencyBenchmark.class.getDeclaredMethod("suppressStdout");
        suppress.setAccessible(true);
        Method restore = ExposureEngineLatencyBenchmark.class.getDeclaredMethod("restoreStdout");
        restore.setAccessible(true);

        java.io.PrintStream original = System.out;
        suppress.invoke(null);
        assertNotEquals(original, System.out);

        restore.invoke(null);
        assertEquals(original, System.out);
    }

    @Test
    void processMessageBinancePartiallyFilled() throws Exception {
        Method method = ExposureEngineLatencyBenchmark.class.getDeclaredMethod(
                "processMessage", OffHeapExposureStore.class, String.class, boolean.class);
        method.setAccessible(true);

        OffHeapExposureStore store = new OffHeapExposureStore();

        String json = MAPPER.writeValueAsString(OrderUpdateKafkaPayload.builder()
                .orderStatus("PARTIALLY_FILLED")
                .quantity("0.5")
                .price("65000.00")
                .pair("B-BTC_USDT")
                .side("sell")
                .timestamp(1000L)
                .build());

        method.invoke(null, store, json, true);

        assertEquals(-0.5, store.getNetQuantity("B-BTC_USDT"), 1e-9);
    }
}
