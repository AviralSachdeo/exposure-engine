package com.github.programmingwithmati.exposureEngine.loadTestPublishers.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.MatchingEngineFill;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateKafkaPayload;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka load-test publisher: sends 10,000 messages for a single pair
 * split across both Binance and ME Kafka topics (5,000 each).
 *
 * Alternates buy/sell, varies quantity and price slightly per message.
 * Prints throughput stats on completion.
 *
 * Env vars:
 *   KAFKA_BOOTSTRAP    – broker address          (default: localhost:9092)
 *   BINANCE_KAFKA_TOPIC – Binance order topic     (default: binance_order_updates)
 *   ME_KAFKA_TOPIC     – ME fills topic           (default: me_fills)
 *   LOAD_TEST_PAIR     – instrument pair to use   (default: B-BTC_USDT)
 *   LOAD_TEST_TOTAL    – total message count      (default: 10000)
 */
public class LoadTestKafkaPublisher {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String bootstrap = envOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        String binanceTopic = envOrDefault("BINANCE_KAFKA_TOPIC", "binance_order_updates");
        String meTopic = envOrDefault("ME_KAFKA_TOPIC", "me_fills");
        String pair = envOrDefault("LOAD_TEST_PAIR", "B-BTC_USDT");
        int totalMessages = Integer.parseInt(envOrDefault("LOAD_TEST_TOTAL", "10000"));

        int binanceCount = totalMessages / 2;
        int meCount = totalMessages - binanceCount;

        KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.LINGER_MS_CONFIG, "5",
                ProducerConfig.BATCH_SIZE_CONFIG, "65536"
        ));

        System.out.println("╔════════════════════════════════════════╗");
        System.out.println("║     KAFKA LOAD TEST PUBLISHER          ║");
        System.out.println("╠════════════════════════════════════════╣");
        System.out.printf("║  Pair         : %-23s║%n", pair);
        System.out.printf("║  Binance msgs : %-23d║%n", binanceCount);
        System.out.printf("║  ME msgs      : %-23d║%n", meCount);
        System.out.printf("║  Total        : %-23d║%n", totalMessages);
        System.out.printf("║  Broker       : %-23s║%n", bootstrap);
        System.out.println("╚════════════════════════════════════════╝");

        AtomicInteger ackCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);
        AtomicLong firstSendNs = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(totalMessages);

        long baseTs = System.currentTimeMillis();
        double basePrice = 65000.0;
        double baseQty = 0.1;

        long startNs = System.nanoTime();
        firstSendNs.set(startNs);

        System.out.println("\nSending " + binanceCount + " Binance order updates...");
        for (int i = 0; i < binanceCount; i++) {
            String side = (i % 2 == 0) ? "buy" : "sell";
            double price = basePrice + (i % 100) * 10.0;
            double qty = baseQty + (i % 50) * 0.01;

            OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                    .derivativesFuturesOrderId("LT-BN-" + i)
                    .quantity(String.format("%.4f", qty))
                    .price(String.format("%.2f", price))
                    .pair(pair)
                    .exchangeTradeId(100000 + i)
                    .ecode("B")
                    .orderStatus("FILLED")
                    .timestamp(baseTs + i)
                    .orderFilledQuantity(String.format("%.4f", qty))
                    .orderAvgPrice(String.format("%.2f", price))
                    .side(side)
                    .isMaker(i % 3 == 0)
                    .build();

            producer.send(
                    new ProducerRecord<>(binanceTopic, pair, toJson(payload)),
                    (meta, ex) -> {
                        if (ex != null) {
                            errorCount.incrementAndGet();
                        } else {
                            ackCount.incrementAndGet();
                        }
                        latch.countDown();
                    });

            if (i > 0 && i % 1000 == 0) {
                System.out.printf("  [Binance] %,d / %,d sent%n", i, binanceCount);
            }
        }

        System.out.println("Sending " + meCount + " ME fill events...");
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

            producer.send(
                    new ProducerRecord<>(meTopic, pair, toJson(fill)),
                    (meta, ex) -> {
                        if (ex != null) {
                            errorCount.incrementAndGet();
                        } else {
                            ackCount.incrementAndGet();
                        }
                        latch.countDown();
                    });

            if (i > 0 && i % 1000 == 0) {
                System.out.printf("  [ME]      %,d / %,d sent%n", i, meCount);
            }
        }

        producer.flush();

        System.out.println("\nWaiting for all acks...");
        latch.await();

        long elapsedNs = System.nanoTime() - startNs;
        double elapsedMs = elapsedNs / 1_000_000.0;
        double elapsedSec = elapsedMs / 1_000.0;
        double throughput = totalMessages / elapsedSec;

        producer.close();

        System.out.println("\n╔════════════════════════════════════════╗");
        System.out.println("║           LOAD TEST RESULTS            ║");
        System.out.println("╠════════════════════════════════════════╣");
        System.out.printf("║  Total sent   : %,d msgs %13s║%n", totalMessages, "");
        System.out.printf("║  Acked        : %,d msgs %13s║%n", ackCount.get(), "");
        System.out.printf("║  Errors       : %,d msgs %17s║%n", errorCount.get(), "");
        System.out.printf("║  Elapsed      : %,.2f ms %14s║%n", elapsedMs, "");
        System.out.printf("║  Throughput   : %,.0f msgs/sec %8s║%n", throughput, "");
        System.out.println("╚════════════════════════════════════════╝");
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
