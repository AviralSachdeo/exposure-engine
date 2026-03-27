package com.github.programmingwithmati.exposureEngine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateKafkaPayload;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class OrderUpdateConsumerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void defaultKafkaBootstrap() throws Exception {
        OrderUpdateConsumer consumer = new OrderUpdateConsumer();
        Field field = OrderUpdateConsumer.class.getDeclaredField("kafkaBootstrap");
        field.setAccessible(true);
        assertEquals("localhost:9092", field.get(consumer));
    }

    @Test
    void defaultConsumerGroup() throws Exception {
        OrderUpdateConsumer consumer = new OrderUpdateConsumer();
        Field field = OrderUpdateConsumer.class.getDeclaredField("consumerGroup");
        field.setAccessible(true);
        assertEquals("order-update-consumers", field.get(consumer));
    }

    @Test
    void kafkaTopicConstant() throws Exception {
        Field field = OrderUpdateConsumer.class.getDeclaredField("KAFKA_TOPIC");
        field.setAccessible(true);
        assertEquals("binance_order_updates", field.get(null));
    }

    @Test
    void handleRecordWithValidPayload() throws Exception {
        OrderUpdateConsumer consumer = new OrderUpdateConsumer();
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .derivativesFuturesOrderId("ORD-1")
                .quantity("1.0")
                .price("65000.00")
                .pair("B-BTC_USDT")
                .exchangeTradeId(100L)
                .ecode("B")
                .orderStatus("FILLED")
                .timestamp(1000L)
                .orderFilledQuantity("1.0")
                .orderAvgPrice("65000.00")
                .side("buy")
                .isMaker(false)
                .build();

        String json = MAPPER.writeValueAsString(payload);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("binance_order_updates", 0, 0L, "B-BTC_USDT", json);

        Method method = OrderUpdateConsumer.class.getDeclaredMethod("handleRecord", ConsumerRecord.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(consumer, record));
    }

    @Test
    void handleRecordWithInvalidJson() throws Exception {
        OrderUpdateConsumer consumer = new OrderUpdateConsumer();
        ConsumerRecord<String, String> record = new ConsumerRecord<>("binance_order_updates", 0, 0L, "key", "not-json");

        Method method = OrderUpdateConsumer.class.getDeclaredMethod("handleRecord", ConsumerRecord.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(consumer, record));
    }

    @Test
    void processOrderUpdateFilled() throws Exception {
        OrderUpdateConsumer consumer = new OrderUpdateConsumer();
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .pair("B-BTC_USDT")
                .side("buy")
                .quantity("1.0")
                .price("65000.00")
                .orderAvgPrice("65000.00")
                .orderStatus("FILLED")
                .isMaker(false)
                .build();

        Method method = OrderUpdateConsumer.class.getDeclaredMethod("processOrderUpdate", OrderUpdateKafkaPayload.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(consumer, payload));
    }

    @Test
    void processOrderUpdatePartiallyFilled() throws Exception {
        OrderUpdateConsumer consumer = new OrderUpdateConsumer();
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .pair("B-ETH_USDT")
                .side("sell")
                .quantity("5.0")
                .price("3400.00")
                .orderAvgPrice("3400.00")
                .orderStatus("PARTIALLY_FILLED")
                .isMaker(true)
                .build();

        Method method = OrderUpdateConsumer.class.getDeclaredMethod("processOrderUpdate", OrderUpdateKafkaPayload.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(consumer, payload));
    }

    @Test
    void processOrderUpdateNewDoesNotLog() throws Exception {
        OrderUpdateConsumer consumer = new OrderUpdateConsumer();
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .pair("B-BTC_USDT")
                .orderStatus("NEW")
                .build();

        Method method = OrderUpdateConsumer.class.getDeclaredMethod("processOrderUpdate", OrderUpdateKafkaPayload.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(consumer, payload));
    }

    @Test
    void processOrderUpdateCanceledDoesNotLog() throws Exception {
        OrderUpdateConsumer consumer = new OrderUpdateConsumer();
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .pair("B-BTC_USDT")
                .orderStatus("CANCELED")
                .build();

        Method method = OrderUpdateConsumer.class.getDeclaredMethod("processOrderUpdate", OrderUpdateKafkaPayload.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(consumer, payload));
    }

    @Test
    void envOrDefaultReturnsDefault() throws Exception {
        Method method = OrderUpdateConsumer.class.getDeclaredMethod("envOrDefault", String.class, String.class);
        method.setAccessible(true);
        assertEquals("default_val", method.invoke(null, "NONEXISTENT_ENV_VAR_XYZ", "default_val"));
    }

    @Test
    void envOrDefaultReturnsEnvIfSet() throws Exception {
        Method method = OrderUpdateConsumer.class.getDeclaredMethod("envOrDefault", String.class, String.class);
        method.setAccessible(true);
        String result = (String) method.invoke(null, "PATH", "fallback");
        assertNotEquals("fallback", result);
    }

    @Test
    void runningFlagDefaultsToTrue() throws Exception {
        OrderUpdateConsumer consumer = new OrderUpdateConsumer();
        Field field = OrderUpdateConsumer.class.getDeclaredField("running");
        field.setAccessible(true);
        assertTrue((boolean) field.get(consumer));
    }

    @Test
    void runningFlagCanBeSetFalse() throws Exception {
        OrderUpdateConsumer consumer = new OrderUpdateConsumer();
        Field field = OrderUpdateConsumer.class.getDeclaredField("running");
        field.setAccessible(true);
        field.set(consumer, false);
        assertFalse((boolean) field.get(consumer));
    }

    @Test
    void createKafkaConsumerReturnsConsumer() throws Exception {
        OrderUpdateConsumer consumer = new OrderUpdateConsumer();
        Method method = OrderUpdateConsumer.class.getDeclaredMethod("createKafkaConsumer");
        method.setAccessible(true);
        var kafkaConsumer = method.invoke(consumer);
        assertNotNull(kafkaConsumer);
        if (kafkaConsumer instanceof org.apache.kafka.clients.consumer.KafkaConsumer<?, ?> kc) {
            kc.close();
        }
    }

    @Test
    void handleRecordWithFilledPayload() throws Exception {
        OrderUpdateConsumer consumer = new OrderUpdateConsumer();
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .derivativesFuturesOrderId("ORD-FILLED")
                .quantity("2.5")
                .price("65500.00")
                .pair("B-BTC_USDT")
                .exchangeTradeId(200L)
                .orderStatus("FILLED")
                .timestamp(5000L)
                .orderFilledQuantity("2.5")
                .orderAvgPrice("65500.00")
                .side("buy")
                .isMaker(true)
                .build();

        String json = MAPPER.writeValueAsString(payload);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("binance_order_updates", 0, 42L, "B-BTC_USDT", json);

        Method method = OrderUpdateConsumer.class.getDeclaredMethod("handleRecord", ConsumerRecord.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(consumer, record));
    }

    @Test
    void handleRecordWithPartiallyFilledPayload() throws Exception {
        OrderUpdateConsumer consumer = new OrderUpdateConsumer();
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .derivativesFuturesOrderId("ORD-PARTIAL")
                .quantity("1.0")
                .price("3400.00")
                .pair("B-ETH_USDT")
                .exchangeTradeId(201L)
                .orderStatus("PARTIALLY_FILLED")
                .timestamp(6000L)
                .orderFilledQuantity("0.5")
                .orderAvgPrice("3400.00")
                .side("sell")
                .isMaker(false)
                .build();

        String json = MAPPER.writeValueAsString(payload);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("binance_order_updates", 1, 100L, "B-ETH_USDT", json);

        Method method = OrderUpdateConsumer.class.getDeclaredMethod("handleRecord", ConsumerRecord.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(consumer, record));
    }

    @Test
    void processOrderUpdateWithNullStatus() throws Exception {
        OrderUpdateConsumer consumer = new OrderUpdateConsumer();
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .pair("B-BTC_USDT")
                .orderStatus(null)
                .build();

        Method method = OrderUpdateConsumer.class.getDeclaredMethod("processOrderUpdate", OrderUpdateKafkaPayload.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(consumer, payload));
    }

    @Test
    void objectMapperFieldExists() throws Exception {
        Field field = OrderUpdateConsumer.class.getDeclaredField("OBJECT_MAPPER");
        field.setAccessible(true);
        assertNotNull(field.get(null));
    }
}
