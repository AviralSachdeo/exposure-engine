package com.github.programmingwithmati.exposureEngine.loadTestExposureEngine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.MatchingEngineFill;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateKafkaPayload;
import com.github.programmingwithmati.exposureEngine.store.OffHeapExposureStore;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end transport latency benchmark for all 4 Binance/ME transport combinations.
 *
 * Measures the complete path: produce → transport → consume → deserialize → applyFill.
 * Embeds {@code System.nanoTime()} in the message timestamp field for correlation.
 *
 * <h3>Scenarios:</h3>
 * <ol>
 *   <li>Binance=Kafka,  ME=Kafka   (requires Kafka broker)</li>
 *   <li>Binance=Kafka,  ME=Aeron   (requires Kafka broker)</li>
 *   <li>Binance=Aeron,  ME=Kafka   (requires Kafka broker)</li>
 *   <li>Binance=Aeron,  ME=Aeron   (no external dependencies)</li>
 * </ol>
 *
 * Aeron uses an embedded MediaDriver with IPC transport (shared memory).
 * If Kafka is unreachable, Kafka-dependent scenarios are skipped automatically.
 *
 * <h3>Env vars:</h3>
 * <pre>
 *   KAFKA_BOOTSTRAP     – broker address  (default: localhost:9092)
 *   LOAD_TEST_DURATION  – seconds / test  (default: 10)
 * </pre>
 */
public class TransportLatencyBenchmark {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String[] PAIRS = {
            "B-BTC_USDT", "B-ETH_USDT", "B-SOL_USDT", "B-BNB_USDT", "B-XRP_USDT",
            "B-ADA_USDT", "B-DOGE_USDT", "B-AVAX_USDT", "B-DOT_USDT", "B-MATIC_USDT"
    };
    private static final int[] THROUGHPUTS = {25_000, 50_000, 100_000};
    private static final int POOL_SIZE = 10_000;
    private static final int BN_AERON_STREAM = 130;
    private static final int ME_AERON_STREAM = 120;

    private static PrintStream OUT;
    private static String kafkaBootstrap;
    private static MediaDriver mediaDriver;
    private static Aeron aeron;

    public static void main(String[] args) throws Exception {
        OUT = System.out;
        int durationSec = intEnv("LOAD_TEST_DURATION", 10);
        kafkaBootstrap = strEnv("KAFKA_BOOTSTRAP", "localhost:9092");

        setupAeron();
        boolean kafkaOk = isKafkaAvailable();

        printBanner(durationSec, kafkaOk);

        OUT.print("Pre-generating payload pools...");
        OrderUpdateKafkaPayload[] bnPool = new OrderUpdateKafkaPayload[POOL_SIZE];
        MatchingEngineFill[] mePool = new MatchingEngineFill[POOL_SIZE];
        generatePools(bnPool, mePool);
        OUT.println(" done.\n");

        String[][] scenarios = {
                {"Kafka/Kafka", "kafka", "kafka"},
                {"Kafka/Aeron", "kafka", "aeron"},
                {"Aeron/Kafka", "aeron", "kafka"},
                {"Aeron/Aeron", "aeron", "aeron"},
        };

        double[][][] results = new double[4][3][];
        boolean[] ran = new boolean[4];

        for (int s = 0; s < 4; s++) {
            String binT = scenarios[s][1];
            String meT = scenarios[s][2];
            boolean needsKafka = "kafka".equals(binT) || "kafka".equals(meT);

            if (needsKafka && !kafkaOk) {
                OUT.printf("%n  !! Skipping %s — Kafka broker not available at %s%n",
                        scenarios[s][0], kafkaBootstrap);
                continue;
            }

            ran[s] = true;

            OUT.printf("%n╔═══════════════════════════════════════════════════════════════════════════╗%n");
            OUT.printf("║  SCENARIO %d: %-15s  (Binance=%-5s  ME=%-5s)                ║%n",
                    s + 1, scenarios[s][0], binT.toUpperCase(), meT.toUpperCase());
            OUT.printf("╚═══════════════════════════════════════════════════════════════════════════╝%n");

            for (int t = 0; t < 3; t++) {
                int target = THROUGHPUTS[t];
                int total = target * durationSec;
                OUT.printf("%n  ▶ %,d msgs/sec  (%,d messages over %ds)%n", target, total, durationSec);
                results[s][t] = runBenchmark(binT, meT, target, durationSec, bnPool, mePool, s, t);
                printResult(results[s][t]);
            }
        }

        printFinalSummary(scenarios, results, ran);

        teardownAeron();
        OUT.println("\nBenchmark complete.\n");
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Core benchmark runner
    // ═══════════════════════════════════════════════════════════════════

    private static double[] runBenchmark(String binTransport, String meTransport,
                                         int targetRate, int durationSec,
                                         OrderUpdateKafkaPayload[] bnPool,
                                         MatchingEngineFill[] mePool,
                                         int scenarioIdx, int throughputIdx) throws Exception {
        int total = targetRate * durationSec;
        int binCount = total / 2;
        int meCount = total - binCount;

        OffHeapExposureStore store = new OffHeapExposureStore();
        long[] latencies = new long[total];
        AtomicInteger latIdx = new AtomicInteger(0);
        CountDownLatch received = new CountDownLatch(total);
        AtomicBoolean running = new AtomicBoolean(true);

        String suffix = "_s" + scenarioIdx + "_t" + throughputIdx + "_" + System.currentTimeMillis();
        String binTopic = "bench_bn" + suffix;
        String meTopic = "bench_me" + suffix;
        String groupSuffix = "grp" + suffix;

        int bnStream = BN_AERON_STREAM + scenarioIdx * 10 + throughputIdx;
        int meStream = ME_AERON_STREAM + scenarioIdx * 10 + throughputIdx;

        suppressStdout();

        Thread bnConsumer = startBinanceConsumer(
                binTransport, binTopic, groupSuffix + "_bn", bnStream,
                store, latencies, latIdx, received, running);
        Thread meConsumer = startMEConsumer(
                meTransport, meTopic, groupSuffix + "_me", meStream,
                store, latencies, latIdx, received, running);

        long settleSleep = ("kafka".equals(binTransport) || "kafka".equals(meTransport)) ? 3000 : 500;
        Thread.sleep(settleSleep);

        long halfIntervalNs = 2_000_000_000L / targetRate;

        Thread bnProducer = startBinanceProducer(binTransport, binTopic, bnStream, binCount, halfIntervalNs, bnPool);
        Thread meProducer = startMEProducer(meTransport, meTopic, meStream, meCount, halfIntervalNs, mePool);

        long benchStart = System.nanoTime();

        bnProducer.join();
        meProducer.join();

        boolean allDone = received.await(durationSec + 30L, TimeUnit.SECONDS);
        long benchElapsed = System.nanoTime() - benchStart;

        running.set(false);
        bnConsumer.join(3000);
        meConsumer.join(3000);

        restoreStdout();

        int actual = latIdx.get();
        if (!allDone) {
            OUT.printf("    !! Received %,d / %,d messages (%.1f%%)%n",
                    actual, total, 100.0 * actual / total);
        }

        return computeStats(latencies, actual, benchElapsed);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Producers
    // ═══════════════════════════════════════════════════════════════════

    private static Thread startBinanceProducer(String transport, String topic, int streamId,
                                               int count, long intervalNs,
                                               OrderUpdateKafkaPayload[] pool) {
        Thread t = new Thread(() -> {
            try {
                if ("kafka".equals(transport)) {
                    produceBinanceKafka(topic, count, intervalNs, pool);
                } else {
                    produceBinanceAeron(streamId, count, intervalNs, pool);
                }
            } catch (Exception e) {
                OUT.println("    ERROR [BN-Producer]: " + e.getMessage());
            }
        }, "bn-producer");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static Thread startMEProducer(String transport, String topic, int streamId,
                                          int count, long intervalNs,
                                          MatchingEngineFill[] pool) {
        Thread t = new Thread(() -> {
            try {
                if ("kafka".equals(transport)) {
                    produceMEKafka(topic, count, intervalNs, pool);
                } else {
                    produceMEAeron(streamId, count, intervalNs, pool);
                }
            } catch (Exception e) {
                OUT.println("    ERROR [ME-Producer]: " + e.getMessage());
            }
        }, "me-producer");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static void produceBinanceKafka(String topic, int count, long intervalNs,
                                            OrderUpdateKafkaPayload[] pool) throws Exception {
        try (KafkaProducer<String, String> producer = createKafkaProducer()) {
            long start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                long sched = start + (long) i * intervalNs;
                while (System.nanoTime() < sched) Thread.onSpinWait();

                OrderUpdateKafkaPayload p = pool[i % POOL_SIZE];
                p.setTimestamp(System.nanoTime());
                String json = MAPPER.writeValueAsString(p);
                producer.send(new ProducerRecord<>(topic, p.getPair(), json));
            }
            producer.flush();
        }
    }

    private static void produceMEKafka(String topic, int count, long intervalNs,
                                       MatchingEngineFill[] pool) throws Exception {
        try (KafkaProducer<String, String> producer = createKafkaProducer()) {
            long start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                long sched = start + (long) i * intervalNs;
                while (System.nanoTime() < sched) Thread.onSpinWait();

                MatchingEngineFill f = pool[i % POOL_SIZE];
                f.setTimestamp(System.nanoTime());
                String json = MAPPER.writeValueAsString(f);
                producer.send(new ProducerRecord<>(topic, f.getPair(), json));
            }
            producer.flush();
        }
    }

    private static void produceBinanceAeron(int streamId, int count, long intervalNs,
                                            OrderUpdateKafkaPayload[] pool) throws Exception {
        try (Publication pub = aeron.addPublication("aeron:ipc", streamId)) {
            UnsafeBuffer buf = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));

            long deadline = System.currentTimeMillis() + 10_000;
            while (!pub.isConnected()) {
                if (System.currentTimeMillis() > deadline) throw new RuntimeException("BN Aeron pub timed out");
                Thread.sleep(10);
            }

            long start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                long sched = start + (long) i * intervalNs;
                while (System.nanoTime() < sched) Thread.onSpinWait();

                OrderUpdateKafkaPayload p = pool[i % POOL_SIZE];
                p.setTimestamp(System.nanoTime());
                byte[] bytes = MAPPER.writeValueAsBytes(p);
                buf.putBytes(0, bytes);

                while (pub.offer(buf, 0, bytes.length) < 0L) Thread.onSpinWait();
            }
        }
    }

    private static void produceMEAeron(int streamId, int count, long intervalNs,
                                       MatchingEngineFill[] pool) throws Exception {
        try (Publication pub = aeron.addPublication("aeron:ipc", streamId)) {
            UnsafeBuffer buf = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));

            long deadline = System.currentTimeMillis() + 10_000;
            while (!pub.isConnected()) {
                if (System.currentTimeMillis() > deadline) throw new RuntimeException("ME Aeron pub timed out");
                Thread.sleep(10);
            }

            long start = System.nanoTime();
            for (int i = 0; i < count; i++) {
                long sched = start + (long) i * intervalNs;
                while (System.nanoTime() < sched) Thread.onSpinWait();

                MatchingEngineFill f = pool[i % POOL_SIZE];
                f.setTimestamp(System.nanoTime());
                byte[] bytes = MAPPER.writeValueAsBytes(f);
                buf.putBytes(0, bytes);

                while (pub.offer(buf, 0, bytes.length) < 0L) Thread.onSpinWait();
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Consumers
    // ═══════════════════════════════════════════════════════════════════

    private static Thread startBinanceConsumer(String transport, String topic, String groupId,
                                               int streamId, OffHeapExposureStore store,
                                               long[] latencies, AtomicInteger latIdx,
                                               CountDownLatch latch, AtomicBoolean running) {
        Thread t = new Thread(() -> {
            try {
                if ("kafka".equals(transport)) {
                    consumeKafkaBinance(topic, groupId, store, latencies, latIdx, latch, running);
                } else {
                    consumeAeronBinance(streamId, store, latencies, latIdx, latch, running);
                }
            } catch (Exception e) {
                if (running.get()) OUT.println("    ERROR [BN-Consumer]: " + e.getMessage());
            }
        }, "bn-consumer");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static Thread startMEConsumer(String transport, String topic, String groupId,
                                          int streamId, OffHeapExposureStore store,
                                          long[] latencies, AtomicInteger latIdx,
                                          CountDownLatch latch, AtomicBoolean running) {
        Thread t = new Thread(() -> {
            try {
                if ("kafka".equals(transport)) {
                    consumeKafkaME(topic, groupId, store, latencies, latIdx, latch, running);
                } else {
                    consumeAeronME(streamId, store, latencies, latIdx, latch, running);
                }
            } catch (Exception e) {
                if (running.get()) OUT.println("    ERROR [ME-Consumer]: " + e.getMessage());
            }
        }, "me-consumer");
        t.setDaemon(true);
        t.start();
        return t;
    }

    private static void consumeKafkaBinance(String topic, String groupId,
                                            OffHeapExposureStore store, long[] latencies,
                                            AtomicInteger latIdx, CountDownLatch latch,
                                            AtomicBoolean running) {
        try (KafkaConsumer<String, String> consumer = createKafkaConsumer(groupId)) {
            consumer.subscribe(List.of(topic));
            while (running.get() || latch.getCount() > 0) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        long now = System.nanoTime();
                        OrderUpdateKafkaPayload p = MAPPER.readValue(record.value(), OrderUpdateKafkaPayload.class);
                        long latency = now - p.getTimestamp();
                        double qty = Double.parseDouble(p.getQuantity());
                        double price = Double.parseDouble(p.getPrice());
                        store.applyFill(p.getPair(), p.getSide(), qty, price, p.getTimestamp(), "BN-K");
                        int idx = latIdx.getAndIncrement();
                        if (idx < latencies.length) latencies[idx] = Math.max(latency, 0);
                        latch.countDown();
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }

    private static void consumeKafkaME(String topic, String groupId,
                                       OffHeapExposureStore store, long[] latencies,
                                       AtomicInteger latIdx, CountDownLatch latch,
                                       AtomicBoolean running) {
        try (KafkaConsumer<String, String> consumer = createKafkaConsumer(groupId)) {
            consumer.subscribe(List.of(topic));
            while (running.get() || latch.getCount() > 0) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        long now = System.nanoTime();
                        MatchingEngineFill f = MAPPER.readValue(record.value(), MatchingEngineFill.class);
                        long latency = now - f.getTimestamp();
                        double qty = Double.parseDouble(f.getQuantity());
                        double price = Double.parseDouble(f.getPrice());
                        store.applyFill(f.getPair(), f.getSide(), qty, price, f.getTimestamp(), "ME-K");
                        int idx = latIdx.getAndIncrement();
                        if (idx < latencies.length) latencies[idx] = Math.max(latency, 0);
                        latch.countDown();
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }

    private static void consumeAeronBinance(int streamId, OffHeapExposureStore store,
                                            long[] latencies, AtomicInteger latIdx,
                                            CountDownLatch latch, AtomicBoolean running) {
        try (Subscription sub = aeron.addSubscription("aeron:ipc", streamId)) {
            while (running.get() || latch.getCount() > 0) {
                int read = sub.poll((buffer, offset, length, header) -> {
                    try {
                        long now = System.nanoTime();
                        byte[] bytes = new byte[length];
                        buffer.getBytes(offset, bytes);
                        OrderUpdateKafkaPayload p = MAPPER.readValue(bytes, OrderUpdateKafkaPayload.class);
                        long latency = now - p.getTimestamp();
                        double qty = Double.parseDouble(p.getQuantity());
                        double price = Double.parseDouble(p.getPrice());
                        store.applyFill(p.getPair(), p.getSide(), qty, price, p.getTimestamp(), "BN-A");
                        int idx = latIdx.getAndIncrement();
                        if (idx < latencies.length) latencies[idx] = Math.max(latency, 0);
                        latch.countDown();
                    } catch (Exception ignored) {
                    }
                }, 100);
                if (read == 0) Thread.onSpinWait();
            }
        }
    }

    private static void consumeAeronME(int streamId, OffHeapExposureStore store,
                                       long[] latencies, AtomicInteger latIdx,
                                       CountDownLatch latch, AtomicBoolean running) {
        try (Subscription sub = aeron.addSubscription("aeron:ipc", streamId)) {
            while (running.get() || latch.getCount() > 0) {
                int read = sub.poll((buffer, offset, length, header) -> {
                    try {
                        long now = System.nanoTime();
                        byte[] bytes = new byte[length];
                        buffer.getBytes(offset, bytes);
                        MatchingEngineFill f = MAPPER.readValue(bytes, MatchingEngineFill.class);
                        long latency = now - f.getTimestamp();
                        double qty = Double.parseDouble(f.getQuantity());
                        double price = Double.parseDouble(f.getPrice());
                        store.applyFill(f.getPair(), f.getSide(), qty, price, f.getTimestamp(), "ME-A");
                        int idx = latIdx.getAndIncrement();
                        if (idx < latencies.length) latencies[idx] = Math.max(latency, 0);
                        latch.countDown();
                    } catch (Exception ignored) {
                    }
                }, 100);
                if (read == 0) Thread.onSpinWait();
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Infrastructure
    // ═══════════════════════════════════════════════════════════════════

    private static void setupAeron() {
        MediaDriver.Context ctx = new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .termBufferSparseFile(false);
        mediaDriver = MediaDriver.launchEmbedded(ctx);
        aeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(mediaDriver.aeronDirectoryName()));
    }

    private static void teardownAeron() {
        if (aeron != null) aeron.close();
        if (mediaDriver != null) mediaDriver.close();
    }

    private static boolean isKafkaAvailable() {
        try {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap);
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
            try (KafkaProducer<String, String> p = new KafkaProducer<>(props)) {
                p.partitionsFor("__bench_connectivity_test");
                return true;
            }
        } catch (Exception e) {
            return false;
        }
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.LINGER_MS_CONFIG, "5",
                ProducerConfig.BATCH_SIZE_CONFIG, "65536"
        ));
    }

    private static KafkaConsumer<String, String> createKafkaConsumer(String groupId) {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000"
        ));
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Data generation
    // ═══════════════════════════════════════════════════════════════════

    private static void generatePools(OrderUpdateKafkaPayload[] bnPool, MatchingEngineFill[] mePool) {
        for (int i = 0; i < POOL_SIZE; i++) {
            String pair = PAIRS[i % PAIRS.length];
            String side = (i & 1) == 0 ? "buy" : "sell";
            double qty = 0.1 + (i % 50) * 0.01;
            double price = 65000.0 + (i % 100) * 10.0;

            bnPool[i] = OrderUpdateKafkaPayload.builder()
                    .derivativesFuturesOrderId("BN-" + i)
                    .quantity(String.format("%.4f", qty))
                    .price(String.format("%.2f", price))
                    .pair(pair)
                    .exchangeTradeId(100_000L + i)
                    .ecode("B")
                    .orderStatus("FILLED")
                    .timestamp(0)
                    .orderFilledQuantity(String.format("%.4f", qty))
                    .orderAvgPrice(String.format("%.2f", price))
                    .side(side)
                    .isMaker(i % 3 == 0)
                    .build();

            mePool[i] = MatchingEngineFill.builder()
                    .derivativesFuturesOrderId("ME-" + i)
                    .quantity(String.format("%.4f", qty))
                    .price(String.format("%.2f", price))
                    .pair(pair)
                    .side(side)
                    .exchangeTradeId("TRD-" + i)
                    .timestamp(0)
                    .isMaker(i % 3 == 0)
                    .build();
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Statistics & output
    // ═══════════════════════════════════════════════════════════════════

    private static double[] computeStats(long[] lat, int n, long elapsedNs) {
        if (n == 0) return new double[]{0, 0, 0, 0, 0, 0};
        Arrays.sort(lat, 0, n);
        double p50 = lat[(int) (n * 0.50)] / 1_000.0;
        double p90 = lat[(int) (n * 0.90)] / 1_000.0;
        double p99 = lat[(int) (n * 0.99)] / 1_000.0;
        double p999 = lat[Math.min((int) (n * 0.999), n - 1)] / 1_000.0;
        long sum = 0;
        for (int i = 0; i < n; i++) sum += lat[i];
        double avg = (sum / (double) n) / 1_000.0;
        double throughput = n / (elapsedNs / 1_000_000_000.0);
        return new double[]{p50, p90, p99, p999, avg, throughput};
    }

    private static void printBanner(int durationSec, boolean kafkaOk) {
        OUT.println();
        OUT.println("╔═══════════════════════════════════════════════════════════════════════════╗");
        OUT.println("║        EXPOSURE ENGINE  —  TRANSPORT LATENCY BENCHMARK                   ║");
        OUT.println("╠═══════════════════════════════════════════════════════════════════════════╣");
        OUT.printf("║  Duration / test   : %-3d seconds                                       ║%n", durationSec);
        OUT.printf("║  Kafka broker      : %-20s %s                     ║%n",
                kafkaBootstrap, kafkaOk ? "[OK]" : "[UNAVAILABLE]");
        OUT.println("║  Aeron transport   : IPC (shared memory, embedded MediaDriver)          ║");
        OUT.println("║  Throughput targets: 25,000 / 50,000 / 100,000 msgs/sec                ║");
        OUT.println("║  Measurement       : end-to-end (produce -> transport -> consume+apply) ║");
        OUT.println("╚═══════════════════════════════════════════════════════════════════════════╝");
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

    private static void printFinalSummary(String[][] scenarios, double[][][] results, boolean[] ran) {
        OUT.println();
        OUT.println("╔═══════════════════════════════════════════════════════════════════════════════════════════╗");
        OUT.println("║                          FINAL SUMMARY  —  END-TO-END LATENCY                            ║");
        OUT.println("╚═══════════════════════════════════════════════════════════════════════════════════════════╝");

        for (int t = 0; t < THROUGHPUTS.length; t++) {
            OUT.printf("%n  ── Target: %,d msgs/sec ──────────────────────────────────────────────────%n", THROUGHPUTS[t]);
            OUT.printf("  ╔═══════════════════╦══════════╦══════════╦══════════╦══════════╦════════════════════╗%n");
            OUT.printf("  ║ %-17s ║ %8s ║ %8s ║ %8s ║ %8s ║ %18s ║%n",
                    "Scenario", "p50(µs)", "p90(µs)", "p99(µs)", "Avg(µs)", "Achieved");
            OUT.printf("  ╠═══════════════════╬══════════╬══════════╬══════════╬══════════╬════════════════════╣%n");
            for (int s = 0; s < 4; s++) {
                if (!ran[s] || results[s][t] == null) continue;
                double[] r = results[s][t];
                OUT.printf("  ║ %-17s ║ %8.2f ║ %8.2f ║ %8.2f ║ %8.2f ║ %,14.0f/s   ║%n",
                        scenarios[s][0], r[0], r[1], r[2], r[4], r[5]);
            }
            OUT.printf("  ╚═══════════════════╩══════════╩══════════╩══════════╩══════════╩════════════════════╝%n");
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  Helpers
    // ═══════════════════════════════════════════════════════════════════

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
            try { return Integer.parseInt(v); } catch (NumberFormatException ignored) {}
        }
        return def;
    }

    private static String strEnv(String key, String def) {
        String v = System.getenv(key);
        return (v != null && !v.isBlank()) ? v : def;
    }
}
