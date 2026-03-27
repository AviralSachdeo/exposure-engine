package com.github.programmingwithmati.exposureEngine.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.*;

class OrderUpdateKafkaPayloadTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void builderCreatesObject() {
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .derivativesFuturesOrderId("BN-ORD-001")
                .quantity("1.2")
                .price("65100.00")
                .pair("B-BTC_USDT")
                .exchangeTradeId(90001)
                .ecode("B")
                .orderStatus("FILLED")
                .timestamp(1000L)
                .orderFilledQuantity("1.2")
                .orderAvgPrice("65100.00")
                .side("buy")
                .isMaker(false)
                .build();

        assertEquals("BN-ORD-001", payload.getDerivativesFuturesOrderId());
        assertEquals("1.2", payload.getQuantity());
        assertEquals("65100.00", payload.getPrice());
        assertEquals("B-BTC_USDT", payload.getPair());
        assertEquals(90001L, payload.getExchangeTradeId());
        assertEquals("B", payload.getEcode());
        assertEquals("FILLED", payload.getOrderStatus());
        assertEquals(1000L, payload.getTimestamp());
        assertEquals("1.2", payload.getOrderFilledQuantity());
        assertEquals("65100.00", payload.getOrderAvgPrice());
        assertEquals("buy", payload.getSide());
        assertFalse(payload.isMaker());
    }

    @Test
    void noArgConstructor() {
        OrderUpdateKafkaPayload payload = new OrderUpdateKafkaPayload();
        assertNull(payload.getPair());
        assertNull(payload.getQuantity());
        assertEquals(0L, payload.getTimestamp());
    }

    @Test
    void allArgConstructor() {
        OrderUpdateKafkaPayload payload = new OrderUpdateKafkaPayload(
                "ORD-1", "1.0", "65000.0", "B-BTC_USDT", 100L, "B",
                "FILLED", 500L, "1.0", "65000.0", "buy", false);

        assertEquals("ORD-1", payload.getDerivativesFuturesOrderId());
        assertEquals("1.0", payload.getQuantity());
        assertEquals("B-BTC_USDT", payload.getPair());
    }

    @Test
    void settersWork() {
        OrderUpdateKafkaPayload payload = new OrderUpdateKafkaPayload();
        payload.setDerivativesFuturesOrderId("ORD-2");
        payload.setQuantity("2.0");
        payload.setPrice("66000.0");
        payload.setPair("B-ETH_USDT");
        payload.setExchangeTradeId(200L);
        payload.setEcode("B");
        payload.setOrderStatus("PARTIALLY_FILLED");
        payload.setTimestamp(999L);
        payload.setOrderFilledQuantity("1.5");
        payload.setOrderAvgPrice("65500.0");
        payload.setSide("sell");
        payload.setMaker(true);

        assertEquals("ORD-2", payload.getDerivativesFuturesOrderId());
        assertEquals("2.0", payload.getQuantity());
        assertEquals("66000.0", payload.getPrice());
        assertEquals("B-ETH_USDT", payload.getPair());
        assertEquals(200L, payload.getExchangeTradeId());
        assertEquals("B", payload.getEcode());
        assertEquals("PARTIALLY_FILLED", payload.getOrderStatus());
        assertEquals(999L, payload.getTimestamp());
        assertEquals("1.5", payload.getOrderFilledQuantity());
        assertEquals("65500.0", payload.getOrderAvgPrice());
        assertEquals("sell", payload.getSide());
        assertTrue(payload.isMaker());
    }

    @Test
    void jsonSerializationRoundTrip() throws Exception {
        OrderUpdateKafkaPayload original = OrderUpdateKafkaPayload.builder()
                .derivativesFuturesOrderId("ORD-1")
                .quantity("1.0")
                .price("65000.0")
                .pair("B-BTC_USDT")
                .exchangeTradeId(100L)
                .ecode("B")
                .orderStatus("FILLED")
                .timestamp(500L)
                .orderFilledQuantity("1.0")
                .orderAvgPrice("65000.0")
                .side("buy")
                .isMaker(true)
                .build();

        String json = MAPPER.writeValueAsString(original);
        OrderUpdateKafkaPayload deserialized = MAPPER.readValue(json, OrderUpdateKafkaPayload.class);

        assertEquals(original, deserialized);
    }

    @Test
    void jsonPropertyNamesCorrect() throws Exception {
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .derivativesFuturesOrderId("ORD-1")
                .quantity("1.0")
                .price("65000.0")
                .pair("B-BTC_USDT")
                .exchangeTradeId(100L)
                .ecode("B")
                .orderStatus("FILLED")
                .timestamp(500L)
                .orderFilledQuantity("1.0")
                .orderAvgPrice("65000.0")
                .side("buy")
                .isMaker(false)
                .build();

        String json = MAPPER.writeValueAsString(payload);

        assertTrue(json.contains("\"derivatives_futures_order_id\""));
        assertTrue(json.contains("\"quantity\""));
        assertTrue(json.contains("\"price\""));
        assertTrue(json.contains("\"pair\""));
        assertTrue(json.contains("\"exchange_trade_id\""));
        assertTrue(json.contains("\"ecode\""));
        assertTrue(json.contains("\"order_status\""));
        assertTrue(json.contains("\"timestamp\""));
        assertTrue(json.contains("\"order_filled_quantity\""));
        assertTrue(json.contains("\"order_avg_price\""));
        assertTrue(json.contains("\"side\""));
        assertTrue(json.contains("\"is_maker\""));
    }

    @Test
    void jsonDeserializationFromSnakeCase() throws Exception {
        String json = """
                {
                    "derivatives_futures_order_id": "ORD-TEST",
                    "quantity": "3.14",
                    "price": "42000.00",
                    "pair": "B-SOL_USDT",
                    "exchange_trade_id": 777,
                    "ecode": "B",
                    "order_status": "PARTIALLY_FILLED",
                    "timestamp": 99999,
                    "order_filled_quantity": "2.0",
                    "order_avg_price": "41500.00",
                    "side": "sell",
                    "is_maker": true
                }
                """;

        OrderUpdateKafkaPayload payload = MAPPER.readValue(json, OrderUpdateKafkaPayload.class);

        assertEquals("ORD-TEST", payload.getDerivativesFuturesOrderId());
        assertEquals("3.14", payload.getQuantity());
        assertEquals("42000.00", payload.getPrice());
        assertEquals("B-SOL_USDT", payload.getPair());
        assertEquals(777L, payload.getExchangeTradeId());
        assertEquals("B", payload.getEcode());
        assertEquals("PARTIALLY_FILLED", payload.getOrderStatus());
        assertEquals(99999L, payload.getTimestamp());
        assertEquals("2.0", payload.getOrderFilledQuantity());
        assertEquals("41500.00", payload.getOrderAvgPrice());
        assertEquals("sell", payload.getSide());
        assertTrue(payload.isMaker());
    }

    @Test
    void fromOrderUpdateEventMapsCorrectly() {
        OrderUpdateEvent.OrderData orderData = OrderUpdateEvent.OrderData.builder()
                .symbol("BTCUSDT")
                .clientOrderId("test-client-order")
                .side("BUY")
                .lastFilledQuantity("1.5")
                .lastFilledPrice("65000.00")
                .averagePrice("64900.00")
                .orderStatus("FILLED")
                .tradeId(12345L)
                .filledAccumulatedQuantity("1.5")
                .makerSide(false)
                .build();

        OrderUpdateEvent event = OrderUpdateEvent.builder()
                .eventType("ORDER_TRADE_UPDATE")
                .eventTime(1000L)
                .transactionTime(1001L)
                .order(orderData)
                .build();

        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.fromOrderUpdateEvent(event);

        assertEquals("test-client-order", payload.getDerivativesFuturesOrderId());
        assertEquals("1.5", payload.getQuantity());
        assertEquals("65000.00", payload.getPrice());
        assertEquals("B-BTC_USDT", payload.getPair());
        assertEquals(12345L, payload.getExchangeTradeId());
        assertEquals("B", payload.getEcode());
        assertEquals("FILLED", payload.getOrderStatus());
        assertEquals(1001L, payload.getTimestamp());
        assertEquals("1.5", payload.getOrderFilledQuantity());
        assertEquals("64900.00", payload.getOrderAvgPrice());
        assertEquals("buy", payload.getSide());
        assertFalse(payload.isMaker());
    }

    @Test
    void fromOrderUpdateEventConvertsSymbolToInternalPair() {
        OrderUpdateEvent.OrderData orderData = OrderUpdateEvent.OrderData.builder()
                .symbol("ETHUSDT")
                .clientOrderId("test-1")
                .side("SELL")
                .lastFilledQuantity("5.0")
                .lastFilledPrice("3400.00")
                .averagePrice("3400.00")
                .orderStatus("PARTIALLY_FILLED")
                .tradeId(99L)
                .filledAccumulatedQuantity("5.0")
                .makerSide(true)
                .build();

        OrderUpdateEvent event = OrderUpdateEvent.builder()
                .transactionTime(2000L)
                .order(orderData)
                .build();

        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.fromOrderUpdateEvent(event);

        assertEquals("B-ETH_USDT", payload.getPair());
        assertEquals("sell", payload.getSide());
        assertTrue(payload.isMaker());
    }

    @ParameterizedTest
    @CsvSource({
            "BTCUSDT,   B-BTC_USDT",
            "ETHUSDT,   B-ETH_USDT",
            "SOLUSDT,   B-SOL_USDT",
            "DOGEUSDT,  B-DOGE_USDT",
            "BTCBUSD,   B-BTC_BUSD",
            "ETHUSDC,   B-ETH_USDC",
            "ETHBTC,    B-ETH_BTC",
            "SOLETH,    B-SOL_ETH",
    })
    void toInstrumentPairConvertsKnownQuoteCurrencies(String binanceSymbol, String expected) throws Exception {
        OrderUpdateEvent.OrderData orderData = OrderUpdateEvent.OrderData.builder()
                .symbol(binanceSymbol)
                .clientOrderId("t")
                .side("BUY")
                .lastFilledQuantity("1.0")
                .lastFilledPrice("100.0")
                .averagePrice("100.0")
                .orderStatus("FILLED")
                .tradeId(1L)
                .filledAccumulatedQuantity("1.0")
                .makerSide(false)
                .build();

        OrderUpdateEvent event = OrderUpdateEvent.builder()
                .transactionTime(1L)
                .order(orderData)
                .build();

        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.fromOrderUpdateEvent(event);
        assertEquals(expected, payload.getPair());
    }

    @Test
    void toInstrumentPairFallsBackForUnknownQuote() {
        OrderUpdateEvent.OrderData orderData = OrderUpdateEvent.OrderData.builder()
                .symbol("XYZABC")
                .clientOrderId("t")
                .side("BUY")
                .lastFilledQuantity("1.0")
                .lastFilledPrice("100.0")
                .averagePrice("100.0")
                .orderStatus("FILLED")
                .tradeId(1L)
                .filledAccumulatedQuantity("1.0")
                .makerSide(false)
                .build();

        OrderUpdateEvent event = OrderUpdateEvent.builder()
                .transactionTime(1L)
                .order(orderData)
                .build();

        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.fromOrderUpdateEvent(event);
        assertEquals("B-XYZABC", payload.getPair());
    }

    @Test
    void fromOrderUpdateEventConvertsSideToLowerCase() {
        OrderUpdateEvent.OrderData orderData = OrderUpdateEvent.OrderData.builder()
                .symbol("BTCUSDT")
                .clientOrderId("t")
                .side("SELL")
                .lastFilledQuantity("1.0")
                .lastFilledPrice("65000.0")
                .averagePrice("65000.0")
                .orderStatus("FILLED")
                .tradeId(1L)
                .filledAccumulatedQuantity("1.0")
                .makerSide(false)
                .build();

        OrderUpdateEvent event = OrderUpdateEvent.builder()
                .transactionTime(1L)
                .order(orderData)
                .build();

        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.fromOrderUpdateEvent(event);
        assertEquals("sell", payload.getSide());
    }

    @Test
    void equalsAndHashCode() {
        OrderUpdateKafkaPayload p1 = OrderUpdateKafkaPayload.builder()
                .pair("B-BTC_USDT").quantity("1.0").side("buy").timestamp(100L).build();
        OrderUpdateKafkaPayload p2 = OrderUpdateKafkaPayload.builder()
                .pair("B-BTC_USDT").quantity("1.0").side("buy").timestamp(100L).build();
        OrderUpdateKafkaPayload p3 = OrderUpdateKafkaPayload.builder()
                .pair("B-ETH_USDT").quantity("2.0").side("sell").timestamp(200L).build();

        assertEquals(p1, p2);
        assertEquals(p1.hashCode(), p2.hashCode());
        assertNotEquals(p1, p3);
    }

    @Test
    void toStringContainsPair() {
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder().pair("B-BTC_USDT").build();
        assertTrue(payload.toString().contains("B-BTC_USDT"));
    }
}
