package com.github.programmingwithmati.exposureEngine.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MatchingEngineFillTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void builderCreatesObject() {
        MatchingEngineFill fill = MatchingEngineFill.builder()
                .derivativesFuturesOrderId("ME-ORD-001")
                .quantity("0.5")
                .price("65200.50")
                .pair("B-BTC_USDT")
                .exchangeTradeId("ME-TRD-1001")
                .side("buy")
                .timestamp(1000L)
                .isMaker(false)
                .build();

        assertEquals("ME-ORD-001", fill.getDerivativesFuturesOrderId());
        assertEquals("0.5", fill.getQuantity());
        assertEquals("65200.50", fill.getPrice());
        assertEquals("B-BTC_USDT", fill.getPair());
        assertEquals("ME-TRD-1001", fill.getExchangeTradeId());
        assertEquals("buy", fill.getSide());
        assertEquals(1000L, fill.getTimestamp());
        assertFalse(fill.isMaker());
    }

    @Test
    void noArgConstructor() {
        MatchingEngineFill fill = new MatchingEngineFill();
        assertNull(fill.getPair());
        assertNull(fill.getQuantity());
        assertNull(fill.getPrice());
        assertEquals(0L, fill.getTimestamp());
    }

    @Test
    void allArgConstructor() {
        MatchingEngineFill fill = new MatchingEngineFill(
                "ORD-1", "1.0", "65000.0", "B-BTC_USDT", "TRD-1", "buy", 500L, true);

        assertEquals("ORD-1", fill.getDerivativesFuturesOrderId());
        assertEquals("1.0", fill.getQuantity());
        assertEquals("65000.0", fill.getPrice());
        assertEquals("B-BTC_USDT", fill.getPair());
        assertEquals("TRD-1", fill.getExchangeTradeId());
        assertEquals("buy", fill.getSide());
        assertEquals(500L, fill.getTimestamp());
        assertTrue(fill.isMaker());
    }

    @Test
    void settersWork() {
        MatchingEngineFill fill = new MatchingEngineFill();
        fill.setDerivativesFuturesOrderId("ORD-2");
        fill.setQuantity("2.0");
        fill.setPrice("66000.0");
        fill.setPair("B-ETH_USDT");
        fill.setExchangeTradeId("TRD-2");
        fill.setSide("sell");
        fill.setTimestamp(999L);
        fill.setMaker(true);

        assertEquals("ORD-2", fill.getDerivativesFuturesOrderId());
        assertEquals("2.0", fill.getQuantity());
        assertEquals("66000.0", fill.getPrice());
        assertEquals("B-ETH_USDT", fill.getPair());
        assertEquals("TRD-2", fill.getExchangeTradeId());
        assertEquals("sell", fill.getSide());
        assertEquals(999L, fill.getTimestamp());
        assertTrue(fill.isMaker());
    }

    @Test
    void jsonSerializationRoundTrip() throws Exception {
        MatchingEngineFill original = MatchingEngineFill.builder()
                .derivativesFuturesOrderId("ME-ORD-001")
                .quantity("0.5")
                .price("65200.50")
                .pair("B-BTC_USDT")
                .exchangeTradeId("ME-TRD-1001")
                .side("buy")
                .timestamp(1000L)
                .isMaker(true)
                .build();

        String json = MAPPER.writeValueAsString(original);
        MatchingEngineFill deserialized = MAPPER.readValue(json, MatchingEngineFill.class);

        assertEquals(original, deserialized);
    }

    @Test
    void jsonPropertyNamesCorrect() throws Exception {
        MatchingEngineFill fill = MatchingEngineFill.builder()
                .derivativesFuturesOrderId("ORD-1")
                .quantity("1.0")
                .price("65000.0")
                .pair("B-BTC_USDT")
                .exchangeTradeId("TRD-1")
                .side("buy")
                .timestamp(100L)
                .isMaker(false)
                .build();

        String json = MAPPER.writeValueAsString(fill);

        assertTrue(json.contains("\"derivatives_futures_order_id\""));
        assertTrue(json.contains("\"quantity\""));
        assertTrue(json.contains("\"price\""));
        assertTrue(json.contains("\"pair\""));
        assertTrue(json.contains("\"exchange_trade_id\""));
        assertTrue(json.contains("\"side\""));
        assertTrue(json.contains("\"timestamp\""));
        assertTrue(json.contains("\"is_maker\""));
    }

    @Test
    void jsonDeserializationFromSnakeCase() throws Exception {
        String json = """
                {
                    "derivatives_futures_order_id": "ME-ORD-999",
                    "quantity": "3.14",
                    "price": "42000.00",
                    "pair": "B-SOL_USDT",
                    "exchange_trade_id": "TRD-999",
                    "side": "sell",
                    "timestamp": 12345,
                    "is_maker": true
                }
                """;

        MatchingEngineFill fill = MAPPER.readValue(json, MatchingEngineFill.class);

        assertEquals("ME-ORD-999", fill.getDerivativesFuturesOrderId());
        assertEquals("3.14", fill.getQuantity());
        assertEquals("42000.00", fill.getPrice());
        assertEquals("B-SOL_USDT", fill.getPair());
        assertEquals("TRD-999", fill.getExchangeTradeId());
        assertEquals("sell", fill.getSide());
        assertEquals(12345L, fill.getTimestamp());
        assertTrue(fill.isMaker());
    }

    @Test
    void ignoresUnknownProperties() throws Exception {
        String json = """
                {
                    "derivatives_futures_order_id": "ORD-1",
                    "quantity": "1.0",
                    "price": "65000.0",
                    "pair": "B-BTC_USDT",
                    "exchange_trade_id": "TRD-1",
                    "side": "buy",
                    "timestamp": 100,
                    "is_maker": false,
                    "unknown_field": "should_be_ignored"
                }
                """;

        assertDoesNotThrow(() -> MAPPER.readValue(json, MatchingEngineFill.class));
    }

    @Test
    void equalsAndHashCode() {
        MatchingEngineFill f1 = MatchingEngineFill.builder()
                .pair("B-BTC_USDT").quantity("1.0").price("65000.0").side("buy").timestamp(100L).build();
        MatchingEngineFill f2 = MatchingEngineFill.builder()
                .pair("B-BTC_USDT").quantity("1.0").price("65000.0").side("buy").timestamp(100L).build();
        MatchingEngineFill f3 = MatchingEngineFill.builder()
                .pair("B-ETH_USDT").quantity("2.0").price("3400.0").side("sell").timestamp(200L).build();

        assertEquals(f1, f2);
        assertEquals(f1.hashCode(), f2.hashCode());
        assertNotEquals(f1, f3);
    }

    @Test
    void toStringContainsPair() {
        MatchingEngineFill fill = MatchingEngineFill.builder().pair("B-BTC_USDT").build();
        assertTrue(fill.toString().contains("B-BTC_USDT"));
    }
}
