package com.github.programmingwithmati.exposureEngine.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class OrderUpdateEventTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void builderCreatesEvent() {
        OrderUpdateEvent.OrderData orderData = OrderUpdateEvent.OrderData.builder()
                .symbol("BTCUSDT")
                .clientOrderId("client-123")
                .side("BUY")
                .orderType("LIMIT")
                .timeInForce("GTC")
                .originalQuantity("1.0")
                .originalPrice("65000.00")
                .averagePrice("65000.00")
                .stopPrice("0.00")
                .executionType("TRADE")
                .orderStatus("FILLED")
                .orderId(12345L)
                .lastFilledQuantity("1.0")
                .filledAccumulatedQuantity("1.0")
                .lastFilledPrice("65000.00")
                .commissionAsset("USDT")
                .commission("6.50")
                .tradeId(99001L)
                .bidsNotional("0.0")
                .askNotional("0.0")
                .makerSide(false)
                .reduceOnly(false)
                .workingType("CONTRACT_PRICE")
                .originalOrderType("LIMIT")
                .positionSide("BOTH")
                .closeAll(false)
                .activationPrice("0")
                .callbackRate("0")
                .priceProtection(false)
                .realizedProfit("0.0")
                .stpMode("NONE")
                .priceMatchMode("NONE")
                .gtdAutoCancelTime(0L)
                .expiryReason("NONE")
                .build();

        OrderUpdateEvent event = OrderUpdateEvent.builder()
                .eventType("ORDER_TRADE_UPDATE")
                .eventTime(1000L)
                .transactionTime(1001L)
                .order(orderData)
                .build();

        assertEquals("ORDER_TRADE_UPDATE", event.getEventType());
        assertEquals(1000L, event.getEventTime());
        assertEquals(1001L, event.getTransactionTime());
        assertNotNull(event.getOrder());
        assertEquals("BTCUSDT", event.getOrder().getSymbol());
        assertEquals("BUY", event.getOrder().getSide());
        assertEquals("FILLED", event.getOrder().getOrderStatus());
    }

    @Test
    void noArgConstructors() {
        OrderUpdateEvent event = new OrderUpdateEvent();
        assertNull(event.getEventType());
        assertNull(event.getOrder());

        OrderUpdateEvent.OrderData data = new OrderUpdateEvent.OrderData();
        assertNull(data.getSymbol());
        assertNull(data.getSide());
    }

    @Test
    void jsonSerializationRoundTrip() throws Exception {
        OrderUpdateEvent.OrderData order = OrderUpdateEvent.OrderData.builder()
                .symbol("ETHUSDT")
                .side("SELL")
                .orderStatus("PARTIALLY_FILLED")
                .lastFilledQuantity("5.0")
                .lastFilledPrice("3400.00")
                .averagePrice("3400.00")
                .clientOrderId("test-order")
                .tradeId(50001L)
                .filledAccumulatedQuantity("5.0")
                .makerSide(true)
                .build();

        OrderUpdateEvent original = OrderUpdateEvent.builder()
                .eventType("ORDER_TRADE_UPDATE")
                .eventTime(2000L)
                .transactionTime(2001L)
                .order(order)
                .build();

        String json = MAPPER.writeValueAsString(original);
        OrderUpdateEvent deserialized = MAPPER.readValue(json, OrderUpdateEvent.class);

        assertEquals(original.getEventType(), deserialized.getEventType());
        assertEquals(original.getEventTime(), deserialized.getEventTime());
        assertEquals(original.getTransactionTime(), deserialized.getTransactionTime());
        assertEquals(original.getOrder().getSymbol(), deserialized.getOrder().getSymbol());
        assertEquals(original.getOrder().getSide(), deserialized.getOrder().getSide());
    }

    @Test
    void jsonPropertyMappings() throws Exception {
        String json = """
                {
                    "e": "ORDER_TRADE_UPDATE",
                    "E": 1000,
                    "T": 1001,
                    "o": {
                        "s": "BTCUSDT",
                        "c": "client-1",
                        "S": "BUY",
                        "o": "LIMIT",
                        "f": "GTC",
                        "q": "1.5",
                        "p": "65000.00",
                        "ap": "65000.00",
                        "sp": "0",
                        "x": "TRADE",
                        "X": "FILLED",
                        "i": 12345,
                        "l": "1.5",
                        "z": "1.5",
                        "L": "65000.00",
                        "N": "USDT",
                        "n": "6.50",
                        "t": 99001,
                        "b": "0",
                        "a": "0",
                        "m": false,
                        "R": false,
                        "wt": "CONTRACT_PRICE",
                        "ot": "LIMIT",
                        "ps": "BOTH",
                        "cp": false,
                        "AP": "0",
                        "cr": "0",
                        "pP": false,
                        "rp": "0.0",
                        "V": "NONE",
                        "pm": "NONE",
                        "gtd": 0,
                        "er": "NONE"
                    }
                }
                """;

        OrderUpdateEvent event = MAPPER.readValue(json, OrderUpdateEvent.class);

        assertEquals("ORDER_TRADE_UPDATE", event.getEventType());
        assertEquals(1000L, event.getEventTime());
        assertEquals(1001L, event.getTransactionTime());

        OrderUpdateEvent.OrderData o = event.getOrder();
        assertEquals("BTCUSDT", o.getSymbol());
        assertEquals("client-1", o.getClientOrderId());
        assertEquals("BUY", o.getSide());
        assertEquals("LIMIT", o.getOrderType());
        assertEquals("GTC", o.getTimeInForce());
        assertEquals("1.5", o.getOriginalQuantity());
        assertEquals("65000.00", o.getOriginalPrice());
        assertEquals("65000.00", o.getAveragePrice());
        assertEquals("0", o.getStopPrice());
        assertEquals("TRADE", o.getExecutionType());
        assertEquals("FILLED", o.getOrderStatus());
        assertEquals(12345L, o.getOrderId());
        assertEquals("1.5", o.getLastFilledQuantity());
        assertEquals("1.5", o.getFilledAccumulatedQuantity());
        assertEquals("65000.00", o.getLastFilledPrice());
        assertEquals("USDT", o.getCommissionAsset());
        assertEquals("6.50", o.getCommission());
        assertEquals(99001L, o.getTradeId());
        assertEquals("0", o.getBidsNotional());
        assertEquals("0", o.getAskNotional());
        assertFalse(o.isMakerSide());
        assertFalse(o.isReduceOnly());
        assertEquals("CONTRACT_PRICE", o.getWorkingType());
        assertEquals("LIMIT", o.getOriginalOrderType());
        assertEquals("BOTH", o.getPositionSide());
        assertFalse(o.isCloseAll());
        assertEquals("0", o.getActivationPrice());
        assertEquals("0", o.getCallbackRate());
        assertFalse(o.isPriceProtection());
        assertEquals("0.0", o.getRealizedProfit());
        assertEquals("NONE", o.getStpMode());
        assertEquals("NONE", o.getPriceMatchMode());
        assertEquals(0L, o.getGtdAutoCancelTime());
        assertEquals("NONE", o.getExpiryReason());
    }

    @Test
    void ignoresUnknownPropertiesOnEvent() throws Exception {
        String json = """
                {
                    "e": "ORDER_TRADE_UPDATE",
                    "E": 1000,
                    "T": 1001,
                    "o": {
                        "s": "BTCUSDT",
                        "S": "BUY",
                        "X": "FILLED",
                        "unknown_nested": "ignored"
                    },
                    "unknown_top": "ignored"
                }
                """;

        assertDoesNotThrow(() -> MAPPER.readValue(json, OrderUpdateEvent.class));
    }

    @Test
    void orderDataSettersWork() {
        OrderUpdateEvent.OrderData data = new OrderUpdateEvent.OrderData();
        data.setSymbol("SOLUSDT");
        data.setClientOrderId("client-99");
        data.setSide("SELL");
        data.setOrderType("MARKET");
        data.setTimeInForce("IOC");
        data.setOriginalQuantity("100.0");
        data.setOriginalPrice("150.00");
        data.setAveragePrice("150.50");
        data.setStopPrice("0");
        data.setExecutionType("TRADE");
        data.setOrderStatus("FILLED");
        data.setOrderId(999L);
        data.setLastFilledQuantity("100.0");
        data.setFilledAccumulatedQuantity("100.0");
        data.setLastFilledPrice("150.50");
        data.setCommissionAsset("USDT");
        data.setCommission("1.50");
        data.setTradeId(88888L);
        data.setBidsNotional("0");
        data.setAskNotional("0");
        data.setMakerSide(true);
        data.setReduceOnly(true);
        data.setWorkingType("MARK_PRICE");
        data.setOriginalOrderType("MARKET");
        data.setPositionSide("LONG");
        data.setCloseAll(true);
        data.setActivationPrice("145.0");
        data.setCallbackRate("1.0");
        data.setPriceProtection(true);
        data.setRealizedProfit("50.0");
        data.setStpMode("CB");
        data.setPriceMatchMode("OPPONENT");
        data.setGtdAutoCancelTime(9999L);
        data.setExpiryReason("CANCELLED");

        assertEquals("SOLUSDT", data.getSymbol());
        assertEquals("client-99", data.getClientOrderId());
        assertEquals("SELL", data.getSide());
        assertEquals("MARKET", data.getOrderType());
        assertEquals("IOC", data.getTimeInForce());
        assertEquals("100.0", data.getOriginalQuantity());
        assertEquals("150.00", data.getOriginalPrice());
        assertEquals("150.50", data.getAveragePrice());
        assertEquals("0", data.getStopPrice());
        assertEquals("TRADE", data.getExecutionType());
        assertEquals("FILLED", data.getOrderStatus());
        assertEquals(999L, data.getOrderId());
        assertEquals("100.0", data.getLastFilledQuantity());
        assertEquals("100.0", data.getFilledAccumulatedQuantity());
        assertEquals("150.50", data.getLastFilledPrice());
        assertEquals("USDT", data.getCommissionAsset());
        assertEquals("1.50", data.getCommission());
        assertEquals(88888L, data.getTradeId());
        assertTrue(data.isMakerSide());
        assertTrue(data.isReduceOnly());
        assertTrue(data.isCloseAll());
        assertTrue(data.isPriceProtection());
        assertEquals("MARK_PRICE", data.getWorkingType());
        assertEquals("LONG", data.getPositionSide());
        assertEquals("145.0", data.getActivationPrice());
        assertEquals("1.0", data.getCallbackRate());
        assertEquals("50.0", data.getRealizedProfit());
        assertEquals("CB", data.getStpMode());
        assertEquals("OPPONENT", data.getPriceMatchMode());
        assertEquals(9999L, data.getGtdAutoCancelTime());
        assertEquals("CANCELLED", data.getExpiryReason());
    }

    @Test
    void eventSettersWork() {
        OrderUpdateEvent event = new OrderUpdateEvent();
        OrderUpdateEvent.OrderData data = new OrderUpdateEvent.OrderData();
        data.setSymbol("BTCUSDT");

        event.setEventType("ORDER_TRADE_UPDATE");
        event.setEventTime(5000L);
        event.setTransactionTime(5001L);
        event.setOrder(data);

        assertEquals("ORDER_TRADE_UPDATE", event.getEventType());
        assertEquals(5000L, event.getEventTime());
        assertEquals(5001L, event.getTransactionTime());
        assertEquals("BTCUSDT", event.getOrder().getSymbol());
    }

    @Test
    void equalsAndHashCode() {
        OrderUpdateEvent.OrderData d1 = OrderUpdateEvent.OrderData.builder().symbol("BTCUSDT").side("BUY").build();
        OrderUpdateEvent.OrderData d2 = OrderUpdateEvent.OrderData.builder().symbol("BTCUSDT").side("BUY").build();

        OrderUpdateEvent e1 = OrderUpdateEvent.builder().eventType("ORDER_TRADE_UPDATE").order(d1).build();
        OrderUpdateEvent e2 = OrderUpdateEvent.builder().eventType("ORDER_TRADE_UPDATE").order(d2).build();

        assertEquals(e1, e2);
        assertEquals(e1.hashCode(), e2.hashCode());
    }

    @Test
    void toStringContainsEventType() {
        OrderUpdateEvent event = OrderUpdateEvent.builder().eventType("ORDER_TRADE_UPDATE").build();
        assertTrue(event.toString().contains("ORDER_TRADE_UPDATE"));
    }

    @Test
    void orderDataToStringContainsSymbol() {
        OrderUpdateEvent.OrderData data = OrderUpdateEvent.OrderData.builder().symbol("BTCUSDT").build();
        assertTrue(data.toString().contains("BTCUSDT"));
    }

    @Test
    void allArgConstructorForOrderData() {
        OrderUpdateEvent.OrderData data = new OrderUpdateEvent.OrderData(
                "BTCUSDT", "client-1", "BUY", "LIMIT", "GTC",
                "1.0", "65000.00", "65000.00", "0",
                "TRADE", "FILLED", 1L, "1.0", "1.0", "65000.00",
                "USDT", "6.50", 99L, "0", "0",
                false, false, "CONTRACT_PRICE", "LIMIT", "BOTH",
                false, "0", "0", false, "0.0",
                "NONE", "NONE", 0L, "NONE"
        );

        assertEquals("BTCUSDT", data.getSymbol());
        assertEquals("client-1", data.getClientOrderId());
        assertEquals("BUY", data.getSide());
        assertEquals("LIMIT", data.getOrderType());
        assertEquals("GTC", data.getTimeInForce());
    }

    @Test
    void allArgConstructorForEvent() {
        OrderUpdateEvent.OrderData data = OrderUpdateEvent.OrderData.builder().symbol("BTCUSDT").build();
        OrderUpdateEvent event = new OrderUpdateEvent("ORDER_TRADE_UPDATE", 1L, 2L, data);

        assertEquals("ORDER_TRADE_UPDATE", event.getEventType());
        assertEquals(1L, event.getEventTime());
        assertEquals(2L, event.getTransactionTime());
        assertEquals("BTCUSDT", event.getOrder().getSymbol());
    }
}
