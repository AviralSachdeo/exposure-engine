package com.github.programmingwithmati.exposureEngine.testPublishers.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateKafkaPayload;
import lombok.SneakyThrows;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Map;

/**
 * Test Kafka producer that publishes sample Binance order-update events
 * to the {@code binance_order_updates} topic.
 *
 * Use with {@code BINANCE_TRANSPORT=kafka} in ExposureEngine.
 */
public class BinanceTestKafkaProducer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TOPIC = "binance_order_updates";

    public static void main(String[] args) {
        String bootstrap = envOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");

        KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        ));

        long now = System.currentTimeMillis();

        List<OrderUpdateKafkaPayload> events = List.of(
                OrderUpdateKafkaPayload.builder()
                        .derivativesFuturesOrderId("BN-ORD-001")
                        .quantity("1.2")
                        .price("65100.00")
                        .pair("B-BTC_USDT")
                        .exchangeTradeId(90001)
                        .ecode("B")
                        .orderStatus("FILLED")
                        .timestamp(now)
                        .orderFilledQuantity("1.2")
                        .orderAvgPrice("65100.00")
                        .side("buy")
                        .isMaker(false)
                        .build(),
                OrderUpdateKafkaPayload.builder()
                        .derivativesFuturesOrderId("BN-ORD-002")
                        .quantity("5.0")
                        .price("3420.50")
                        .pair("B-ETH_USDT")
                        .exchangeTradeId(90002)
                        .ecode("B")
                        .orderStatus("FILLED")
                        .timestamp(now + 1)
                        .orderFilledQuantity("5.0")
                        .orderAvgPrice("3420.50")
                        .side("sell")
                        .isMaker(true)
                        .build(),
                OrderUpdateKafkaPayload.builder()
                        .derivativesFuturesOrderId("BN-ORD-003")
                        .quantity("0.8")
                        .price("65250.00")
                        .pair("B-BTC_USDT")
                        .exchangeTradeId(90003)
                        .ecode("B")
                        .orderStatus("PARTIALLY_FILLED")
                        .timestamp(now + 2)
                        .orderFilledQuantity("0.8")
                        .orderAvgPrice("65250.00")
                        .side("sell")
                        .isMaker(false)
                        .build(),
                OrderUpdateKafkaPayload.builder()
                        .derivativesFuturesOrderId("BN-ORD-004")
                        .quantity("200.0")
                        .price("0.1580")
                        .pair("B-DOGE_USDT")
                        .exchangeTradeId(90004)
                        .ecode("B")
                        .orderStatus("FILLED")
                        .timestamp(now + 3)
                        .orderFilledQuantity("200.0")
                        .orderAvgPrice("0.1580")
                        .side("buy")
                        .isMaker(true)
                        .build(),
                OrderUpdateKafkaPayload.builder()
                        .derivativesFuturesOrderId("BN-ORD-005")
                        .quantity("50.0")
                        .price("142.90")
                        .pair("B-LTC_USDT")
                        .exchangeTradeId(90005)
                        .ecode("B")
                        .orderStatus("FILLED")
                        .timestamp(now + 4)
                        .orderFilledQuantity("50.0")
                        .orderAvgPrice("142.90")
                        .side("buy")
                        .isMaker(false)
                        .build()
        );

        for (OrderUpdateKafkaPayload event : events) {
            String value = toJson(event);
            producer.send(new ProducerRecord<>(TOPIC, event.getPair(), value),
                    (meta, ex) -> {
                        if (ex != null) {
                            System.err.println("Send failed: " + ex.getMessage());
                        } else {
                            System.out.printf("Sent to %s [p=%d, o=%d] | pair=%s side=%s qty=%s price=%s status=%s%n",
                                    TOPIC, meta.partition(), meta.offset(),
                                    event.getPair(), event.getSide(), event.getQuantity(),
                                    event.getPrice(), event.getOrderStatus());
                        }
                    });
        }

        producer.flush();
        producer.close();
        System.out.println("All " + events.size() + " Binance test events sent to [" + TOPIC + "].");
    }

    @SneakyThrows
    private static String toJson(OrderUpdateKafkaPayload payload) {
        return OBJECT_MAPPER.writeValueAsString(payload);
    }

    private static String envOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
