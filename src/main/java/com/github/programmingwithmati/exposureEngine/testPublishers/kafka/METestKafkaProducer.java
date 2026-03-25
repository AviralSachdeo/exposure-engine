package com.github.programmingwithmati.exposureEngine.testPublishers.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.MatchingEngineFill;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Map;

/**
 * Test Kafka producer that publishes sample matching-engine fill events
 * to the {@code me_fills} topic.
 *
 * Use with {@code ME_TRANSPORT=kafka} in ExposureEngine.
 */
public class METestKafkaProducer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TOPIC = "me_fills";

    public static void main(String[] args) {
        String bootstrap = envOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");

        KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        ));

        long now = System.currentTimeMillis();

        List<MatchingEngineFill> fills = List.of(
                MatchingEngineFill.builder()
                        .derivativesFuturesOrderId("ME-ORD-101")
                        .quantity("0.3")
                        .price("65150.00")
                        .pair("B-BTC_USDT")
                        .side("buy")
                        .exchangeTradeId("ME-TRD-2001")
                        .timestamp(now)
                        .isMaker(false)
                        .build(),
                MatchingEngineFill.builder()
                        .derivativesFuturesOrderId("ME-ORD-102")
                        .quantity("8.0")
                        .price("3415.75")
                        .pair("B-ETH_USDT")
                        .side("buy")
                        .exchangeTradeId("ME-TRD-2002")
                        .timestamp(now + 1)
                        .isMaker(true)
                        .build(),
                MatchingEngineFill.builder()
                        .derivativesFuturesOrderId("ME-ORD-103")
                        .quantity("1.5")
                        .price("65300.00")
                        .pair("B-BTC_USDT")
                        .side("sell")
                        .exchangeTradeId("ME-TRD-2003")
                        .timestamp(now + 2)
                        .isMaker(false)
                        .build(),
                MatchingEngineFill.builder()
                        .derivativesFuturesOrderId("ME-ORD-104")
                        .quantity("3000.0")
                        .price("0.1610")
                        .pair("B-DOGE_USDT")
                        .side("buy")
                        .exchangeTradeId("ME-TRD-2004")
                        .timestamp(now + 3)
                        .isMaker(true)
                        .build(),
                MatchingEngineFill.builder()
                        .derivativesFuturesOrderId("ME-ORD-105")
                        .quantity("75.0")
                        .price("143.20")
                        .pair("B-LTC_USDT")
                        .side("sell")
                        .exchangeTradeId("ME-TRD-2005")
                        .timestamp(now + 4)
                        .isMaker(false)
                        .build()
        );

        for (MatchingEngineFill fill : fills) {
            String value = toJson(fill);
            producer.send(new ProducerRecord<>(TOPIC, fill.getPair(), value),
                    (meta, ex) -> {
                        if (ex != null) {
                            System.err.println("Send failed: " + ex.getMessage());
                        } else {
                            System.out.printf("Sent to %s [p=%d, o=%d] | pair=%s side=%s qty=%s price=%s%n",
                                    TOPIC, meta.partition(), meta.offset(),
                                    fill.getPair(), fill.getSide(), fill.getQuantity(), fill.getPrice());
                        }
                    });
        }

        producer.flush();
        producer.close();
        System.out.println("All " + fills.size() + " ME test fills sent to [" + TOPIC + "].");
    }

    @SneakyThrows
    private static String toJson(MatchingEngineFill fill) {
        return OBJECT_MAPPER.writeValueAsString(fill);
    }

    private static String envOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
