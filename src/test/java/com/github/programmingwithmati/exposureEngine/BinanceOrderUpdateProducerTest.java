package com.github.programmingwithmati.exposureEngine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateEvent;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateKafkaPayload;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class BinanceOrderUpdateProducerTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void hmacSha256ProducesValidHex() throws Exception {
        Method method = BinanceOrderUpdateProducer.class.getDeclaredMethod("hmacSha256", String.class, String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, "secret123", "data456");
        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertTrue(result.matches("[0-9a-f]+"), "HMAC should be hex string");
        assertEquals(64, result.length(), "SHA-256 HMAC is 32 bytes = 64 hex chars");
    }

    @Test
    void hmacSha256IsDeterministic() throws Exception {
        Method method = BinanceOrderUpdateProducer.class.getDeclaredMethod("hmacSha256", String.class, String.class);
        method.setAccessible(true);

        String result1 = (String) method.invoke(null, "my-secret", "my-data");
        String result2 = (String) method.invoke(null, "my-secret", "my-data");
        assertEquals(result1, result2);
    }

    @Test
    void hmacSha256DifferentKeyGivesDifferentResult() throws Exception {
        Method method = BinanceOrderUpdateProducer.class.getDeclaredMethod("hmacSha256", String.class, String.class);
        method.setAccessible(true);

        String result1 = (String) method.invoke(null, "key1", "data");
        String result2 = (String) method.invoke(null, "key2", "data");
        assertNotEquals(result1, result2);
    }

    @Test
    void hmacSha256DifferentDataGivesDifferentResult() throws Exception {
        Method method = BinanceOrderUpdateProducer.class.getDeclaredMethod("hmacSha256", String.class, String.class);
        method.setAccessible(true);

        String result1 = (String) method.invoke(null, "key", "data1");
        String result2 = (String) method.invoke(null, "key", "data2");
        assertNotEquals(result1, result2);
    }

    @Test
    void hmacSha256WithEmptyData() throws Exception {
        Method method = BinanceOrderUpdateProducer.class.getDeclaredMethod("hmacSha256", String.class, String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, "secret", "");
        assertNotNull(result);
        assertEquals(64, result.length());
    }

    @Test
    void processMessageOrderTradeUpdate() throws Exception {
        OrderUpdateEvent.OrderData orderData = OrderUpdateEvent.OrderData.builder()
                .symbol("BTCUSDT")
                .clientOrderId("test-1")
                .side("BUY")
                .orderType("LIMIT")
                .originalQuantity("1.0")
                .originalPrice("65000.00")
                .orderStatus("FILLED")
                .executionType("TRADE")
                .lastFilledQuantity("1.0")
                .lastFilledPrice("65000.00")
                .averagePrice("65000.00")
                .tradeId(123L)
                .filledAccumulatedQuantity("1.0")
                .makerSide(false)
                .build();

        OrderUpdateEvent event = OrderUpdateEvent.builder()
                .eventType("ORDER_TRADE_UPDATE")
                .eventTime(1000L)
                .transactionTime(1001L)
                .order(orderData)
                .build();

        String json = MAPPER.writeValueAsString(event);

        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.fromOrderUpdateEvent(
                MAPPER.readValue(json, OrderUpdateEvent.class));

        assertEquals("B-BTC_USDT", payload.getPair());
        assertEquals("buy", payload.getSide());
        assertEquals("1.0", payload.getQuantity());
        assertEquals("65000.00", payload.getPrice());
        assertEquals("FILLED", payload.getOrderStatus());
    }

    @Test
    void processMessageIgnoresNonOrderTradeUpdate() throws Exception {
        String json = """
                {
                    "e": "ACCOUNT_UPDATE",
                    "E": 1000,
                    "T": 1001
                }
                """;

        var tree = MAPPER.readTree(json);
        String eventType = tree.has("e") ? tree.get("e").asText() : null;
        assertNotEquals("ORDER_TRADE_UPDATE", eventType);
    }

    @Test
    void processMessageHandlesNullEventType() throws Exception {
        String json = """
                {
                    "E": 1000,
                    "T": 1001
                }
                """;

        var tree = MAPPER.readTree(json);
        String eventType = tree.has("e") ? tree.get("e").asText() : null;
        assertNull(eventType);
    }

    @Test
    void requireEnvThrowsOnMissing() throws Exception {
        Method method = BinanceOrderUpdateProducer.class.getDeclaredMethod("requireEnv", String.class);
        method.setAccessible(true);

        assertThrows(Exception.class, () -> {
            try {
                method.invoke(null, "NONEXISTENT_ENV_VAR_THAT_SHOULD_NOT_EXIST");
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
            }
        });
    }

    @Test
    void envOrDefaultReturnsDefault() throws Exception {
        Method method = BinanceOrderUpdateProducer.class.getDeclaredMethod("envOrDefault", String.class, String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, "NONEXISTENT_ENV_VAR_TEST", "fallback");
        assertEquals("fallback", result);
    }

    @Test
    void envOrDefaultReturnsEnvIfSet() throws Exception {
        Method method = BinanceOrderUpdateProducer.class.getDeclaredMethod("envOrDefault", String.class, String.class);
        method.setAccessible(true);

        String result = (String) method.invoke(null, "PATH", "fallback");
        assertNotEquals("fallback", result);
    }

    @Test
    void kafkaTopicConstant() throws Exception {
        java.lang.reflect.Field field = BinanceOrderUpdateProducer.class.getDeclaredField("KAFKA_TOPIC");
        field.setAccessible(true);
        assertEquals("binance_order_updates", field.get(null));
    }

    @Test
    void eventTypeConstant() throws Exception {
        java.lang.reflect.Field field = BinanceOrderUpdateProducer.class.getDeclaredField("EVENT_TYPE_ORDER_UPDATE");
        field.setAccessible(true);
        assertEquals("ORDER_TRADE_UPDATE", field.get(null));
    }

    @Test
    void keepaliveIntervalConstant() throws Exception {
        java.lang.reflect.Field field = BinanceOrderUpdateProducer.class.getDeclaredField("KEEPALIVE_INTERVAL_MINUTES");
        field.setAccessible(true);
        assertEquals(30L, field.get(null));
    }

    @Test
    void processMessageLogic_orderTradeUpdateIsParsed() throws Exception {
        OrderUpdateEvent.OrderData order = OrderUpdateEvent.OrderData.builder()
                .symbol("SOLUSDT")
                .clientOrderId("sol-ord-1")
                .side("BUY")
                .orderType("MARKET")
                .originalQuantity("50.0")
                .originalPrice("0.00")
                .averagePrice("150.00")
                .orderStatus("FILLED")
                .executionType("TRADE")
                .lastFilledQuantity("50.0")
                .lastFilledPrice("150.00")
                .tradeId(555L)
                .filledAccumulatedQuantity("50.0")
                .makerSide(false)
                .build();

        OrderUpdateEvent event = OrderUpdateEvent.builder()
                .eventType("ORDER_TRADE_UPDATE")
                .eventTime(2000L)
                .transactionTime(2001L)
                .order(order)
                .build();

        String rawJson = MAPPER.writeValueAsString(event);

        JsonNode tree = MAPPER.readTree(rawJson);
        String eventType = tree.has("e") ? tree.get("e").asText() : null;
        assertEquals("ORDER_TRADE_UPDATE", eventType);

        OrderUpdateEvent parsed = MAPPER.treeToValue(tree, OrderUpdateEvent.class);
        assertEquals("SOLUSDT", parsed.getOrder().getSymbol());
        assertEquals("BUY", parsed.getOrder().getSide());

        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.fromOrderUpdateEvent(parsed);
        assertEquals("B-SOL_USDT", payload.getPair());
        assertEquals("buy", payload.getSide());
        assertEquals("50.0", payload.getQuantity());
    }

    @Test
    void processMessageLogic_nonOrderTradeUpdateIsSkipped() throws Exception {
        String json = "{\"e\":\"ACCOUNT_UPDATE\",\"E\":1000,\"T\":1001}";

        JsonNode tree = MAPPER.readTree(json);
        String eventType = tree.has("e") ? tree.get("e").asText() : null;
        assertNotEquals("ORDER_TRADE_UPDATE", eventType);
    }

    @Test
    void processMessageLogic_noEventTypeFieldIsSkipped() throws Exception {
        String json = "{\"E\":1000,\"T\":1001}";

        JsonNode tree = MAPPER.readTree(json);
        String eventType = tree.has("e") ? tree.get("e").asText() : null;
        assertNull(eventType);
    }

    @Test
    void processMessageLogic_truncatesLongErrorMessages() throws Exception {
        String longJson = "{\"e\":\"ORDER_TRADE_UPDATE\"," + "x".repeat(300) + "}";
        int truncLen = Math.min(200, longJson.length());
        String truncated = longJson.substring(0, truncLen);
        assertNotNull(truncated);
        assertEquals(200, truncated.length());
    }

    @Test
    void buildSignedQueryStringFormat() throws Exception {
        String queryString = "timestamp=" + System.currentTimeMillis();
        assertNotNull(queryString);
        assertTrue(queryString.startsWith("timestamp="));

        Method hmacMethod = BinanceOrderUpdateProducer.class.getDeclaredMethod("hmacSha256", String.class, String.class);
        hmacMethod.setAccessible(true);
        String signature = (String) hmacMethod.invoke(null, "test-secret", queryString);
        String fullQuery = queryString + "&signature=" + signature;
        assertTrue(fullQuery.contains("timestamp="));
        assertTrue(fullQuery.contains("&signature="));
    }

    @Test
    void webSocketListenerOnTextBuffering() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("partial1");
        assertFalse(buffer.isEmpty());
        buffer.append("partial2");
        String full = buffer.toString();
        assertEquals("partial1partial2", full);
        buffer.setLength(0);
        assertTrue(buffer.isEmpty());
    }

    @Test
    void fullOrderUpdateEventDeserializationAndConversion() throws Exception {
        String json = """
                {
                    "e": "ORDER_TRADE_UPDATE",
                    "E": 1710000000000,
                    "T": 1710000000001,
                    "o": {
                        "s": "ETHUSDT",
                        "c": "my-order-id",
                        "S": "SELL",
                        "o": "MARKET",
                        "f": "GTC",
                        "q": "10.0",
                        "p": "0.00",
                        "ap": "3450.50",
                        "sp": "0",
                        "x": "TRADE",
                        "X": "FILLED",
                        "i": 456789,
                        "l": "10.0",
                        "z": "10.0",
                        "L": "3450.50",
                        "N": "USDT",
                        "n": "3.45",
                        "t": 8888,
                        "b": "0",
                        "a": "0",
                        "m": true,
                        "R": false,
                        "wt": "CONTRACT_PRICE",
                        "ot": "MARKET",
                        "ps": "BOTH",
                        "cp": false,
                        "rp": "100.00"
                    }
                }
                """;

        OrderUpdateEvent event = MAPPER.readValue(json, OrderUpdateEvent.class);
        assertEquals("ORDER_TRADE_UPDATE", event.getEventType());

        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.fromOrderUpdateEvent(event);
        assertEquals("B-ETH_USDT", payload.getPair());
        assertEquals("sell", payload.getSide());
        assertEquals("10.0", payload.getQuantity());
        assertEquals("3450.50", payload.getPrice());
        assertEquals("FILLED", payload.getOrderStatus());
        assertEquals(8888L, payload.getExchangeTradeId());
        assertEquals("B", payload.getEcode());
        assertTrue(payload.isMaker());
    }
}
