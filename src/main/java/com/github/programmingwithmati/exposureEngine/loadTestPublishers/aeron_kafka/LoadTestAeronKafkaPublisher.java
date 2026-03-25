package com.github.programmingwithmati.exposureEngine.loadTestPublishers.aeron_kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.MatchingEngineFill;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateKafkaPayload;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import lombok.SneakyThrows;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Hybrid load-test publisher: Binance via Aeron, ME via Kafka.
 *
 * Sends 10,000 messages (5,000 Binance order updates over Aeron +
 * 5,000 ME fills over Kafka) for a single pair.
 *
 * Use with ExposureEngine config: BINANCE_TRANSPORT=aeron, ME_TRANSPORT=kafka
 *
 * Env vars:
 *   KAFKA_BOOTSTRAP         – broker address              (default: localhost:9092)
 *   ME_KAFKA_TOPIC          – ME fills topic               (default: me_fills)
 *   BINANCE_AERON_CHANNEL   – Binance Aeron channel        (default: aeron:udp?endpoint=localhost:40457)
 *   BINANCE_AERON_STREAM_ID – Binance Aeron stream id      (default: 30)
 *   LOAD_TEST_PAIR          – instrument pair               (default: B-BTC_USDT)
 *   LOAD_TEST_TOTAL         – total message count           (default: 10000)
 */
public class LoadTestAeronKafkaPublisher {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String bootstrap = envOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        String meTopic = envOrDefault("ME_KAFKA_TOPIC", "me_fills");
        String binanceChannel = envOrDefault("BINANCE_AERON_CHANNEL", "aeron:udp?endpoint=localhost:40457");
        int binanceStreamId = Integer.parseInt(envOrDefault("BINANCE_AERON_STREAM_ID", "30"));
        String pair = envOrDefault("LOAD_TEST_PAIR", "B-BTC_USDT");
        int totalMessages = Integer.parseInt(envOrDefault("LOAD_TEST_TOTAL", "10000"));

        int binanceCount = totalMessages / 2;
        int meCount = totalMessages - binanceCount;

        System.out.println("╔══════════════════════════════════════════════╗");
        System.out.println("║  LOAD TEST: Binance=AERON  ME=KAFKA          ║");
        System.out.println("╠══════════════════════════════════════════════╣");
        System.out.printf("║  Pair           : %-26s║%n", pair);
        System.out.printf("║  Binance(Aeron) : %,d msgs → %-15s║%n", binanceCount, binanceChannel);
        System.out.printf("║  ME(Kafka)      : %,d msgs → %-15s║%n", meCount, meTopic);
        System.out.printf("║  Total          : %,d msgs %17s║%n", totalMessages, "");
        System.out.println("╚══════════════════════════════════════════════╝");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.LINGER_MS_CONFIG, "5",
                ProducerConfig.BATCH_SIZE_CONFIG, "65536"
        ));

        MediaDriver.Context driverCtx = new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        try (MediaDriver driver = MediaDriver.launchEmbedded(driverCtx);
             Aeron aeron = Aeron.connect(new Aeron.Context()
                     .aeronDirectoryName(driver.aeronDirectoryName()));
             Publication binancePub = aeron.addPublication(binanceChannel, binanceStreamId)) {

            System.out.println("\nBinance Aeron pub: " + binanceChannel + " stream=" + binanceStreamId);
            System.out.println("Waiting for Binance Aeron subscriber...");
            waitForSubscriber(binancePub, "Binance");

            UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(64 * 1024));
            AtomicInteger kafkaAcks = new AtomicInteger(0);
            AtomicInteger kafkaErrors = new AtomicInteger(0);
            CountDownLatch kafkaLatch = new CountDownLatch(meCount);

            long baseTs = System.currentTimeMillis();
            double basePrice = 65000.0;
            double baseQty = 0.1;
            int backPressureCount = 0;

            long startNs = System.nanoTime();

            int binanceSent = 0;
            System.out.println("\nSending " + binanceCount + " Binance order updates via Aeron...");
            for (int i = 0; i < binanceCount; i++) {
                String side = (i % 2 == 0) ? "buy" : "sell";
                double price = basePrice + (i % 100) * 10.0;
                double qty = baseQty + (i % 50) * 0.01;

                OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                        .derivativesFuturesOrderId("LT-BN-" + i)
                        .quantity(String.format("%.4f", qty))
                        .price(String.format("%.2f", price))
                        .pair(pair)
                        .exchangeTradeId(400000 + i)
                        .ecode("B")
                        .orderStatus("FILLED")
                        .timestamp(baseTs + i)
                        .orderFilledQuantity(String.format("%.4f", qty))
                        .orderAvgPrice(String.format("%.2f", price))
                        .side(side)
                        .isMaker(i % 3 == 0)
                        .build();

                byte[] jsonBytes = OBJECT_MAPPER.writeValueAsBytes(payload);
                buffer.putBytes(0, jsonBytes);

                long result;
                while ((result = binancePub.offer(buffer, 0, jsonBytes.length)) < 0L) {
                    if (result == Publication.CLOSED) {
                        System.err.println("Binance publication closed, aborting.");
                        return;
                    }
                    backPressureCount++;
                    Thread.yield();
                }
                binanceSent++;

                if (binanceSent % 1000 == 0) {
                    System.out.printf("  [Binance/Aeron] %,d / %,d sent%n", binanceSent, binanceCount);
                }
            }

            System.out.println("Sending " + meCount + " ME fill events via Kafka...");
            for (int i = 0; i < meCount; i++) {
                String side = (i % 2 == 0) ? "buy" : "sell";
                double price = basePrice + (i % 100) * 10.0;
                double qty = baseQty + (i % 50) * 0.01;

                MatchingEngineFill fill = MatchingEngineFill.builder()
                        .derivativesFuturesOrderId("LT-ME-" + i)
                        .quantity(String.format("%.4f", qty))
                        .price(String.format("%.2f", price))
                        .pair(pair)
                        .side(side)
                        .exchangeTradeId("LT-TRD-" + i)
                        .timestamp(baseTs + binanceCount + i)
                        .isMaker(i % 3 == 0)
                        .build();

                kafkaProducer.send(
                        new ProducerRecord<>(meTopic, pair, toJson(fill)),
                        (meta, ex) -> {
                            if (ex != null) kafkaErrors.incrementAndGet();
                            else kafkaAcks.incrementAndGet();
                            kafkaLatch.countDown();
                        });

                if (i > 0 && i % 1000 == 0) {
                    System.out.printf("  [ME/Kafka]      %,d / %,d sent%n", i, meCount);
                }
            }
            kafkaProducer.flush();

            System.out.println("\nWaiting for Kafka acks...");
            kafkaLatch.await();

            long elapsedNs = System.nanoTime() - startNs;
            double elapsedMs = elapsedNs / 1_000_000.0;
            double elapsedSec = elapsedMs / 1_000.0;
            double throughput = totalMessages / elapsedSec;

            kafkaProducer.close();

            System.out.println("\n╔══════════════════════════════════════════════╗");
            System.out.println("║              LOAD TEST RESULTS               ║");
            System.out.println("╠══════════════════════════════════════════════╣");
            System.out.printf("║  Binance/Aeron : %,d sent, %,d backpressure ║%n",
                    binanceSent, backPressureCount);
            System.out.printf("║  ME/Kafka      : %,d acked, %,d errors %5s║%n",
                    kafkaAcks.get(), kafkaErrors.get(), "");
            System.out.printf("║  Total         : %,d msgs %18s║%n", totalMessages, "");
            System.out.printf("║  Elapsed       : %,.2f ms %17s║%n", elapsedMs, "");
            System.out.printf("║  Throughput    : %,.0f msgs/sec %11s║%n", throughput, "");
            System.out.println("╚══════════════════════════════════════════════╝");

            Thread.sleep(2000);
        }
    }

    private static void waitForSubscriber(Publication publication, String label) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (!publication.isConnected()) {
            if (System.currentTimeMillis() > deadline) {
                System.err.println("[" + label + "] Timed out waiting for subscriber. "
                        + "Is ExposureEngine running with BINANCE_TRANSPORT=aeron?");
                System.exit(1);
            }
            Thread.sleep(100);
        }
        System.out.println("  [" + label + "] subscriber connected.");
    }

    @SneakyThrows
    private static String toJson(Object obj) {
        return OBJECT_MAPPER.writeValueAsString(obj);
    }

    private static String envOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
