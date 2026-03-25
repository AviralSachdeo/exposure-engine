package com.github.programmingwithmati.exposureEngine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateKafkaPayload;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * Kafka consumer that reads ORDER_TRADE_UPDATE events from the
 * {@code binance_order_updates} topic produced by {@link BinanceOrderUpdateProducer}.
 *
 * Env vars:
 *   KAFKA_BOOTSTRAP  – Kafka broker address (default: localhost:9092)
 *   CONSUMER_GROUP   – Consumer group id  (default: order-update-consumers)
 */
public class OrderUpdateConsumer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String KAFKA_TOPIC = "binance_order_updates";

    private final String kafkaBootstrap;
    private final String consumerGroup;
    private volatile boolean running = true;

    public OrderUpdateConsumer() {
        this.kafkaBootstrap = envOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        this.consumerGroup = envOrDefault("CONSUMER_GROUP", "order-update-consumers");
    }

    public static void main(String[] args) {
        OrderUpdateConsumer consumer = new OrderUpdateConsumer();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> consumer.running = false));
        consumer.start();
    }

    public void start() {
        try (KafkaConsumer<String, String> consumer = createKafkaConsumer()) {
            consumer.subscribe(List.of(KAFKA_TOPIC));
            System.out.println("OrderUpdateConsumer subscribed to [" + KAFKA_TOPIC + "]. Waiting for events...");

            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    handleRecord(record);
                }
            }
        }
        System.out.println("OrderUpdateConsumer stopped.");
    }

    private void handleRecord(ConsumerRecord<String, String> record) {
        try {
            OrderUpdateKafkaPayload payload = OBJECT_MAPPER.readValue(
                    record.value(), OrderUpdateKafkaPayload.class);

            System.out.printf(
                    "Consumed | partition=%d offset=%d | pair=%s %s qty=%s price=%s avgPrice=%s " +
                            "filledQty=%s status=%s orderId=%s tradeId=%d%n",
                    record.partition(), record.offset(),
                    payload.getPair(), payload.getSide(),
                    payload.getQuantity(), payload.getPrice(), payload.getOrderAvgPrice(),
                    payload.getOrderFilledQuantity(), payload.getOrderStatus(),
                    payload.getDerivativesFuturesOrderId(), payload.getExchangeTradeId()
            );

            processOrderUpdate(payload);
        } catch (Exception e) {
            System.err.println("Error deserializing record at offset " + record.offset() + ": " + e.getMessage());
        }
    }

    /**
     * Extension point: plug in exposure-state updates, risk calculations, etc.
     */
    private void processOrderUpdate(OrderUpdateKafkaPayload payload) {
        if ("FILLED".equals(payload.getOrderStatus()) || "PARTIALLY_FILLED".equals(payload.getOrderStatus())) {
            System.out.printf("  -> FILL detected: %s %s qty=%s price=%s avgPrice=%s isMaker=%s%n",
                    payload.getPair(), payload.getSide(),
                    payload.getQuantity(), payload.getPrice(),
                    payload.getOrderAvgPrice(), payload.isMaker());
        }
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroup,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"
        ));
    }

    private static String envOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
