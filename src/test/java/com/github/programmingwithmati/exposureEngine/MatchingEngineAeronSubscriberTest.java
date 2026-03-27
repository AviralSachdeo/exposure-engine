package com.github.programmingwithmati.exposureEngine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.MatchingEngineFill;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.*;

class MatchingEngineAeronSubscriberTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void defaultChannelConfig() throws Exception {
        MatchingEngineAeronSubscriber subscriber = new MatchingEngineAeronSubscriber();
        Field field = MatchingEngineAeronSubscriber.class.getDeclaredField("channel");
        field.setAccessible(true);
        assertEquals("aeron:udp?endpoint=localhost:40456", field.get(subscriber));
    }

    @Test
    void defaultStreamIdConfig() throws Exception {
        MatchingEngineAeronSubscriber subscriber = new MatchingEngineAeronSubscriber();
        Field field = MatchingEngineAeronSubscriber.class.getDeclaredField("streamId");
        field.setAccessible(true);
        assertEquals(20, field.get(subscriber));
    }

    @Test
    void handleMessageValidJson() throws Exception {
        MatchingEngineAeronSubscriber subscriber = new MatchingEngineAeronSubscriber();

        MatchingEngineFill fill = MatchingEngineFill.builder()
                .derivativesFuturesOrderId("ME-1")
                .quantity("0.5")
                .price("65200.50")
                .pair("B-BTC_USDT")
                .exchangeTradeId("TRD-1")
                .side("buy")
                .timestamp(1000L)
                .isMaker(false)
                .build();

        String json = MAPPER.writeValueAsString(fill);

        Method method = MatchingEngineAeronSubscriber.class.getDeclaredMethod("handleMessage", String.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(subscriber, json));
    }

    @Test
    void handleMessageInvalidJson() throws Exception {
        MatchingEngineAeronSubscriber subscriber = new MatchingEngineAeronSubscriber();

        Method method = MatchingEngineAeronSubscriber.class.getDeclaredMethod("handleMessage", String.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(subscriber, "not-valid-json"));
    }

    @Test
    void handleMessageLongInvalidJson() throws Exception {
        MatchingEngineAeronSubscriber subscriber = new MatchingEngineAeronSubscriber();
        String longInvalid = "x".repeat(500);

        Method method = MatchingEngineAeronSubscriber.class.getDeclaredMethod("handleMessage", String.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(subscriber, longInvalid));
    }

    @Test
    void processFillDoesNotThrow() throws Exception {
        MatchingEngineAeronSubscriber subscriber = new MatchingEngineAeronSubscriber();

        MatchingEngineFill fill = MatchingEngineFill.builder()
                .pair("B-BTC_USDT")
                .side("buy")
                .quantity("1.0")
                .price("65000.00")
                .timestamp(1000L)
                .build();

        Method method = MatchingEngineAeronSubscriber.class.getDeclaredMethod("processFill", MatchingEngineFill.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(subscriber, fill));
    }

    @Test
    void envOrDefaultReturnsDefault() throws Exception {
        Method method = MatchingEngineAeronSubscriber.class.getDeclaredMethod("envOrDefault", String.class, String.class);
        method.setAccessible(true);
        assertEquals("default_val", method.invoke(null, "NONEXISTENT_ENV_VAR_QRS", "default_val"));
    }

    @Test
    void envOrDefaultReturnsEnvIfSet() throws Exception {
        Method method = MatchingEngineAeronSubscriber.class.getDeclaredMethod("envOrDefault", String.class, String.class);
        method.setAccessible(true);
        String result = (String) method.invoke(null, "PATH", "fallback");
        assertNotEquals("fallback", result);
    }

    @Test
    void runningFlagDefaultsToTrue() throws Exception {
        MatchingEngineAeronSubscriber subscriber = new MatchingEngineAeronSubscriber();
        Field field = MatchingEngineAeronSubscriber.class.getDeclaredField("running");
        field.setAccessible(true);
        assertTrue((boolean) field.get(subscriber));
    }

    @Test
    void handleMessageWithAllFields() throws Exception {
        MatchingEngineAeronSubscriber subscriber = new MatchingEngineAeronSubscriber();

        MatchingEngineFill fill = MatchingEngineFill.builder()
                .derivativesFuturesOrderId("ME-ORD-999")
                .quantity("10.0")
                .price("3400.25")
                .pair("B-ETH_USDT")
                .exchangeTradeId("ME-TRD-9999")
                .side("sell")
                .timestamp(99999L)
                .isMaker(true)
                .build();

        String json = MAPPER.writeValueAsString(fill);

        Method method = MatchingEngineAeronSubscriber.class.getDeclaredMethod("handleMessage", String.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(subscriber, json));
    }

    @Test
    void runningFlagCanBeSetFalse() throws Exception {
        MatchingEngineAeronSubscriber subscriber = new MatchingEngineAeronSubscriber();
        Field field = MatchingEngineAeronSubscriber.class.getDeclaredField("running");
        field.setAccessible(true);
        field.set(subscriber, false);
        assertFalse((boolean) field.get(subscriber));
    }

    @Test
    void handleMessageWithMinimalFill() throws Exception {
        MatchingEngineAeronSubscriber subscriber = new MatchingEngineAeronSubscriber();

        MatchingEngineFill fill = MatchingEngineFill.builder()
                .pair("B-DOGE_USDT")
                .side("buy")
                .quantity("1000.0")
                .price("0.15")
                .timestamp(1L)
                .build();

        String json = MAPPER.writeValueAsString(fill);

        Method method = MatchingEngineAeronSubscriber.class.getDeclaredMethod("handleMessage", String.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(subscriber, json));
    }

    @Test
    void handleMessageEmptyJson() throws Exception {
        MatchingEngineAeronSubscriber subscriber = new MatchingEngineAeronSubscriber();

        Method method = MatchingEngineAeronSubscriber.class.getDeclaredMethod("handleMessage", String.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(subscriber, "{}"));
    }

    @Test
    void objectMapperFieldExists() throws Exception {
        Field field = MatchingEngineAeronSubscriber.class.getDeclaredField("OBJECT_MAPPER");
        field.setAccessible(true);
        assertNotNull(field.get(null));
    }

    @Test
    void processFillWithSellSide() throws Exception {
        MatchingEngineAeronSubscriber subscriber = new MatchingEngineAeronSubscriber();

        MatchingEngineFill fill = MatchingEngineFill.builder()
                .pair("B-SOL_USDT")
                .side("sell")
                .quantity("200.0")
                .price("150.00")
                .timestamp(5000L)
                .isMaker(true)
                .build();

        Method method = MatchingEngineAeronSubscriber.class.getDeclaredMethod("processFill", MatchingEngineFill.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(subscriber, fill));
    }
}
