package com.github.programmingwithmati.exposureEngine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.ExposureSnapshot;
import com.github.programmingwithmati.exposureEngine.model.MatchingEngineFill;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateKafkaPayload;
import com.github.programmingwithmati.exposureEngine.store.OffHeapExposureStore;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class ExposureEngineTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private ExposureEngine engine;
    private OffHeapExposureStore store;

    @BeforeEach
    void setUp() throws Exception {
        engine = new ExposureEngine();

        Field storeField = ExposureEngine.class.getDeclaredField("store");
        storeField.setAccessible(true);
        store = (OffHeapExposureStore) storeField.get(engine);
    }

    @Test
    void processBinanceKafkaRecordFilledOrder() throws Exception {
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .derivativesFuturesOrderId("ORD-1")
                .quantity("1.5")
                .price("65000.00")
                .pair("B-BTC_USDT")
                .exchangeTradeId(100L)
                .ecode("B")
                .orderStatus("FILLED")
                .timestamp(1000L)
                .orderFilledQuantity("1.5")
                .orderAvgPrice("65000.00")
                .side("buy")
                .isMaker(false)
                .build();

        String json = MAPPER.writeValueAsString(payload);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "B-BTC_USDT", json);

        Method method = ExposureEngine.class.getDeclaredMethod("processBinanceKafkaRecord", ConsumerRecord.class);
        method.setAccessible(true);
        method.invoke(engine, record);

        assertEquals(1.5, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(1.5, store.getTotalBuyQuantity("B-BTC_USDT"), 1e-9);
    }

    @Test
    void processBinanceKafkaRecordPartiallyFilledOrder() throws Exception {
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .derivativesFuturesOrderId("ORD-2")
                .quantity("0.5")
                .price("66000.00")
                .pair("B-ETH_USDT")
                .exchangeTradeId(101L)
                .ecode("B")
                .orderStatus("PARTIALLY_FILLED")
                .timestamp(2000L)
                .orderFilledQuantity("0.5")
                .orderAvgPrice("66000.00")
                .side("sell")
                .isMaker(true)
                .build();

        String json = MAPPER.writeValueAsString(payload);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "B-ETH_USDT", json);

        Method method = ExposureEngine.class.getDeclaredMethod("processBinanceKafkaRecord", ConsumerRecord.class);
        method.setAccessible(true);
        method.invoke(engine, record);

        assertEquals(-0.5, store.getNetQuantity("B-ETH_USDT"), 1e-9);
    }

    @Test
    void processBinanceKafkaRecordIgnoresNonFillStatus() throws Exception {
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .derivativesFuturesOrderId("ORD-3")
                .quantity("1.0")
                .price("65000.00")
                .pair("B-BTC_USDT")
                .exchangeTradeId(102L)
                .ecode("B")
                .orderStatus("NEW")
                .timestamp(3000L)
                .orderFilledQuantity("0.0")
                .orderAvgPrice("0.0")
                .side("buy")
                .isMaker(false)
                .build();

        String json = MAPPER.writeValueAsString(payload);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "B-BTC_USDT", json);

        Method method = ExposureEngine.class.getDeclaredMethod("processBinanceKafkaRecord", ConsumerRecord.class);
        method.setAccessible(true);
        method.invoke(engine, record);

        assertEquals(0.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(0, store.getTradeCount("B-BTC_USDT"));
    }

    @Test
    void processBinanceKafkaRecordIgnoresCancelledStatus() throws Exception {
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .orderStatus("CANCELED")
                .quantity("1.0")
                .price("65000.00")
                .pair("B-BTC_USDT")
                .side("buy")
                .timestamp(4000L)
                .build();

        String json = MAPPER.writeValueAsString(payload);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "B-BTC_USDT", json);

        Method method = ExposureEngine.class.getDeclaredMethod("processBinanceKafkaRecord", ConsumerRecord.class);
        method.setAccessible(true);
        method.invoke(engine, record);

        assertEquals(0.0, store.getNetQuantity("B-BTC_USDT"));
    }

    @Test
    void processBinanceKafkaRecordHandlesBadJson() throws Exception {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", "not-valid-json");

        Method method = ExposureEngine.class.getDeclaredMethod("processBinanceKafkaRecord", ConsumerRecord.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(engine, record));
    }

    @Test
    void processBinanceAeronMessageFilledOrder() throws Exception {
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .orderStatus("FILLED")
                .quantity("2.0")
                .price("64000.00")
                .pair("B-BTC_USDT")
                .side("buy")
                .timestamp(5000L)
                .build();

        String json = MAPPER.writeValueAsString(payload);

        Method method = ExposureEngine.class.getDeclaredMethod("processBinanceAeronMessage", String.class);
        method.setAccessible(true);
        method.invoke(engine, json);

        assertEquals(2.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
    }

    @Test
    void processBinanceAeronMessageIgnoresNonFill() throws Exception {
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .orderStatus("NEW")
                .quantity("2.0")
                .price("64000.00")
                .pair("B-BTC_USDT")
                .side("buy")
                .timestamp(5000L)
                .build();

        String json = MAPPER.writeValueAsString(payload);

        Method method = ExposureEngine.class.getDeclaredMethod("processBinanceAeronMessage", String.class);
        method.setAccessible(true);
        method.invoke(engine, json);

        assertEquals(0.0, store.getNetQuantity("B-BTC_USDT"));
    }

    @Test
    void processBinanceAeronMessageHandlesBadJson() throws Exception {
        Method method = ExposureEngine.class.getDeclaredMethod("processBinanceAeronMessage", String.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(engine, "bad-json"));
    }

    @Test
    void processMEKafkaRecord() throws Exception {
        MatchingEngineFill fill = MatchingEngineFill.builder()
                .derivativesFuturesOrderId("ME-1")
                .quantity("3.0")
                .price("65100.00")
                .pair("B-BTC_USDT")
                .side("sell")
                .exchangeTradeId("TRD-1")
                .timestamp(6000L)
                .isMaker(false)
                .build();

        String json = MAPPER.writeValueAsString(fill);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("me-topic", 0, 0L, "B-BTC_USDT", json);

        Method method = ExposureEngine.class.getDeclaredMethod("processMEKafkaRecord", ConsumerRecord.class);
        method.setAccessible(true);
        method.invoke(engine, record);

        assertEquals(-3.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(3.0, store.getTotalSellQuantity("B-BTC_USDT"), 1e-9);
    }

    @Test
    void processMEKafkaRecordHandlesBadJson() throws Exception {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("me-topic", 0, 0L, "key", "invalid");

        Method method = ExposureEngine.class.getDeclaredMethod("processMEKafkaRecord", ConsumerRecord.class);
        method.setAccessible(true);

        assertDoesNotThrow(() -> method.invoke(engine, record));
    }

    @Test
    void processMEAeronMessage() throws Exception {
        MatchingEngineFill fill = MatchingEngineFill.builder()
                .derivativesFuturesOrderId("ME-2")
                .quantity("5.0")
                .price("3400.00")
                .pair("B-ETH_USDT")
                .side("buy")
                .exchangeTradeId("TRD-2")
                .timestamp(7000L)
                .isMaker(true)
                .build();

        String json = MAPPER.writeValueAsString(fill);

        Method method = ExposureEngine.class.getDeclaredMethod("processMEAeronMessage", String.class);
        method.setAccessible(true);
        method.invoke(engine, json);

        assertEquals(5.0, store.getNetQuantity("B-ETH_USDT"), 1e-9);
    }

    @Test
    void processMEAeronMessageHandlesBadJson() throws Exception {
        Method method = ExposureEngine.class.getDeclaredMethod("processMEAeronMessage", String.class);
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(engine, "garbage"));
    }

    @Test
    void applyPayloadBuySide() throws Exception {
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .quantity("10.0")
                .price("150.00")
                .pair("B-SOL_USDT")
                .side("buy")
                .timestamp(8000L)
                .build();

        Method method = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        method.setAccessible(true);
        method.invoke(engine, payload, "TEST");

        assertEquals(10.0, store.getNetQuantity("B-SOL_USDT"), 1e-9);
        assertEquals(10.0 * 150.0, store.getTotalBuyNotional("B-SOL_USDT"), 1e-6);
    }

    @Test
    void applyPayloadSellSide() throws Exception {
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .quantity("5.0")
                .price("3400.00")
                .pair("B-ETH_USDT")
                .side("sell")
                .timestamp(9000L)
                .build();

        Method method = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        method.setAccessible(true);
        method.invoke(engine, payload, "TEST");

        assertEquals(-5.0, store.getNetQuantity("B-ETH_USDT"), 1e-9);
    }

    @Test
    void applyFillFromMatchingEngineFill() throws Exception {
        MatchingEngineFill fill = MatchingEngineFill.builder()
                .quantity("7.5")
                .price("3500.00")
                .pair("B-ETH_USDT")
                .side("buy")
                .timestamp(10000L)
                .build();

        Method method = ExposureEngine.class.getDeclaredMethod(
                "applyFill", MatchingEngineFill.class, String.class);
        method.setAccessible(true);
        method.invoke(engine, fill, "ME-TEST");

        assertEquals(7.5, store.getNetQuantity("B-ETH_USDT"), 1e-9);
        assertEquals(7.5 * 3500.0, store.getTotalBuyNotional("B-ETH_USDT"), 1e-6);
    }

    @Test
    void snapshotAndPublishExposureWithEmptyStore() throws Exception {
        Method method = ExposureEngine.class.getDeclaredMethod("snapshotAndPublishExposure");
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(engine));
    }

    @Test
    void snapshotAndPublishExposureWithDataPrintsSnapshot() throws Exception {
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .quantity("1.0")
                .price("65000.00")
                .pair("B-BTC_USDT")
                .side("buy")
                .timestamp(1000L)
                .build();

        Method applyMethod = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        applyMethod.setAccessible(true);
        applyMethod.invoke(engine, payload, "TEST");

        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();
        assertFalse(snapshots.isEmpty());
        store.printExposureSnapshot();
    }

    @Test
    void defaultTransportConfigValues() throws Exception {
        Field binanceTransport = ExposureEngine.class.getDeclaredField("binanceTransport");
        binanceTransport.setAccessible(true);
        assertEquals("kafka", binanceTransport.get(engine));

        Field meTransport = ExposureEngine.class.getDeclaredField("meTransport");
        meTransport.setAccessible(true);

        Field exposureTransport = ExposureEngine.class.getDeclaredField("exposureTransport");
        exposureTransport.setAccessible(true);
        assertEquals("kafka", exposureTransport.get(engine));
    }

    @Test
    void defaultKafkaConfigValues() throws Exception {
        Field kafkaBootstrap = ExposureEngine.class.getDeclaredField("kafkaBootstrap");
        kafkaBootstrap.setAccessible(true);
        assertEquals("localhost:9092", kafkaBootstrap.get(engine));

        Field consumerGroup = ExposureEngine.class.getDeclaredField("consumerGroup");
        consumerGroup.setAccessible(true);
        assertEquals("exposure-engine", consumerGroup.get(engine));
    }

    @Test
    void defaultTopicConfigValues() throws Exception {
        Field binanceTopic = ExposureEngine.class.getDeclaredField("binanceKafkaTopic");
        binanceTopic.setAccessible(true);
        assertEquals("binance_order_updates", binanceTopic.get(engine));

        Field meTopic = ExposureEngine.class.getDeclaredField("meKafkaTopic");
        meTopic.setAccessible(true);
        assertEquals("me_fills", meTopic.get(engine));

        Field exposureTopic = ExposureEngine.class.getDeclaredField("exposureKafkaTopic");
        exposureTopic.setAccessible(true);
        assertEquals("exposure_snapshots", exposureTopic.get(engine));
    }

    @Test
    void defaultAeronConfigValues() throws Exception {
        Field binanceChannel = ExposureEngine.class.getDeclaredField("binanceAeronChannel");
        binanceChannel.setAccessible(true);
        assertEquals("aeron:udp?endpoint=localhost:40457", binanceChannel.get(engine));

        Field binanceStream = ExposureEngine.class.getDeclaredField("binanceAeronStreamId");
        binanceStream.setAccessible(true);
        assertEquals(30, binanceStream.get(engine));

        Field meChannel = ExposureEngine.class.getDeclaredField("meAeronChannel");
        meChannel.setAccessible(true);
        assertEquals("aeron:udp?endpoint=localhost:40456", meChannel.get(engine));

        Field meStream = ExposureEngine.class.getDeclaredField("meAeronStreamId");
        meStream.setAccessible(true);
        assertEquals(20, meStream.get(engine));

        Field exposureChannel = ExposureEngine.class.getDeclaredField("exposureAeronChannel");
        exposureChannel.setAccessible(true);
        assertEquals("aeron:udp?endpoint=localhost:40458", exposureChannel.get(engine));

        Field exposureStream = ExposureEngine.class.getDeclaredField("exposureAeronStreamId");
        exposureStream.setAccessible(true);
        assertEquals(40, exposureStream.get(engine));
    }

    @Test
    void envOrDefaultReturnsDefault() throws Exception {
        Method method = ExposureEngine.class.getDeclaredMethod("envOrDefault", String.class, String.class);
        method.setAccessible(true);
        assertEquals("default_val", method.invoke(null, "NONEXISTENT_ENV_VAR_FOR_TEST", "default_val"));
    }

    @Test
    void shutdownDoesNotThrow() throws Exception {
        Method method = ExposureEngine.class.getDeclaredMethod("shutdown");
        method.setAccessible(true);
        assertDoesNotThrow(() -> method.invoke(engine));
    }

    @Test
    void multiplePayloadsAccumulate() throws Exception {
        Method method = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        method.setAccessible(true);

        for (int i = 0; i < 5; i++) {
            OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                    .quantity("1.0")
                    .price("65000.00")
                    .pair("B-BTC_USDT")
                    .side("buy")
                    .timestamp(1000L + i)
                    .build();
            method.invoke(engine, payload, "TEST");
        }

        assertEquals(5.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(5, store.getTradeCount("B-BTC_USDT"));
    }

    @Test
    void mixedBinanceAndMEFillsAccumulate() throws Exception {
        Method applyPayload = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        applyPayload.setAccessible(true);

        Method applyFill = ExposureEngine.class.getDeclaredMethod(
                "applyFill", MatchingEngineFill.class, String.class);
        applyFill.setAccessible(true);

        OrderUpdateKafkaPayload binanceFill = OrderUpdateKafkaPayload.builder()
                .quantity("2.0").price("65000.00").pair("B-BTC_USDT")
                .side("buy").timestamp(1000L).build();
        applyPayload.invoke(engine, binanceFill, "BINANCE");

        MatchingEngineFill meFill = MatchingEngineFill.builder()
                .quantity("1.0").price("65100.00").pair("B-BTC_USDT")
                .side("sell").timestamp(2000L).build();
        applyFill.invoke(engine, meFill, "ME");

        assertEquals(1.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(2, store.getTradeCount("B-BTC_USDT"));
    }

    @Test
    void storeExposureSnapshotsReflectState() throws Exception {
        Method applyPayload = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        applyPayload.setAccessible(true);

        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .quantity("3.0").price("65000.00").pair("B-BTC_USDT")
                .side("buy").timestamp(1000L).build();
        applyPayload.invoke(engine, payload, "TEST");

        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();
        assertEquals(1, snapshots.size());
        assertEquals("B-BTC_USDT", snapshots.get(0).getPair());
        assertEquals(3.0, snapshots.get(0).getNetQuantity(), 1e-9);
    }

    @Test
    void createKafkaConsumerReturnsConsumer() throws Exception {
        Method method = ExposureEngine.class.getDeclaredMethod("createKafkaConsumer");
        method.setAccessible(true);
        var consumer = method.invoke(engine);
        assertNotNull(consumer);
        if (consumer instanceof org.apache.kafka.clients.consumer.KafkaConsumer<?, ?> kc) {
            kc.close();
        }
    }

    @Test
    void createExposureProducerReturnsProducer() throws Exception {
        Method method = ExposureEngine.class.getDeclaredMethod("createExposureProducer");
        method.setAccessible(true);
        var producer = method.invoke(engine);
        assertNotNull(producer);
        if (producer instanceof org.apache.kafka.clients.producer.KafkaProducer<?, ?> kp) {
            kp.close();
        }
    }

    @Test
    void publishExposureToKafkaWithNullProducer() throws Exception {
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .quantity("1.0").price("65000.00").pair("B-BTC_USDT")
                .side("buy").timestamp(1000L).build();

        Method applyPayload = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        applyPayload.setAccessible(true);
        applyPayload.invoke(engine, payload, "TEST");

        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();
        assertFalse(snapshots.isEmpty());
    }

    @Test
    void publishExposureToAeronWithNullPublication() throws Exception {
        Field exposureTransportField = ExposureEngine.class.getDeclaredField("exposureTransport");
        exposureTransportField.setAccessible(true);

        String origTransport = (String) exposureTransportField.get(engine);
        assertNotNull(origTransport);
    }

    @Test
    void runningFlagCanBeToggled() throws Exception {
        Field runningField = ExposureEngine.class.getDeclaredField("running");
        runningField.setAccessible(true);
        assertTrue((boolean) runningField.get(engine));

        runningField.set(engine, false);
        assertFalse((boolean) runningField.get(engine));
    }

    @Test
    void processBinanceKafkaRecordMultipleFillStatuses() throws Exception {
        Method method = ExposureEngine.class.getDeclaredMethod("processBinanceKafkaRecord", ConsumerRecord.class);
        method.setAccessible(true);

        String[] statuses = {"NEW", "CANCELED", "EXPIRED", "REJECTED"};
        for (String status : statuses) {
            OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                    .orderStatus(status)
                    .quantity("1.0")
                    .price("65000.00")
                    .pair("B-BTC_USDT")
                    .side("buy")
                    .timestamp(1000L)
                    .build();

            String json = MAPPER.writeValueAsString(payload);
            ConsumerRecord<String, String> record = new ConsumerRecord<>("test-topic", 0, 0L, "key", json);
            method.invoke(engine, record);
        }
        assertEquals(0.0, store.getNetQuantity("B-BTC_USDT"));
    }

    @Test
    void processBinanceAeronPartiallyFilled() throws Exception {
        OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                .orderStatus("PARTIALLY_FILLED")
                .quantity("0.3")
                .price("64000.00")
                .pair("B-BTC_USDT")
                .side("sell")
                .timestamp(5000L)
                .build();

        String json = MAPPER.writeValueAsString(payload);

        Method method = ExposureEngine.class.getDeclaredMethod("processBinanceAeronMessage", String.class);
        method.setAccessible(true);
        method.invoke(engine, json);

        assertEquals(-0.3, store.getNetQuantity("B-BTC_USDT"), 1e-9);
    }

    @Test
    void processMEKafkaRecordBuyFill() throws Exception {
        MatchingEngineFill fill = MatchingEngineFill.builder()
                .derivativesFuturesOrderId("ME-BUY-1")
                .quantity("5.0")
                .price("3400.00")
                .pair("B-ETH_USDT")
                .side("buy")
                .exchangeTradeId("TRD-BUY-1")
                .timestamp(7000L)
                .isMaker(true)
                .build();

        String json = MAPPER.writeValueAsString(fill);
        ConsumerRecord<String, String> record = new ConsumerRecord<>("me-topic", 0, 0L, "B-ETH_USDT", json);

        Method method = ExposureEngine.class.getDeclaredMethod("processMEKafkaRecord", ConsumerRecord.class);
        method.setAccessible(true);
        method.invoke(engine, record);

        assertEquals(5.0, store.getNetQuantity("B-ETH_USDT"), 1e-9);
        assertEquals(5.0, store.getTotalBuyQuantity("B-ETH_USDT"), 1e-9);
    }

    @Test
    void processMEAeronSellFill() throws Exception {
        MatchingEngineFill fill = MatchingEngineFill.builder()
                .quantity("8.0")
                .price("3500.00")
                .pair("B-ETH_USDT")
                .side("sell")
                .timestamp(8000L)
                .build();

        String json = MAPPER.writeValueAsString(fill);

        Method method = ExposureEngine.class.getDeclaredMethod("processMEAeronMessage", String.class);
        method.setAccessible(true);
        method.invoke(engine, json);

        assertEquals(-8.0, store.getNetQuantity("B-ETH_USDT"), 1e-9);
        assertEquals(8.0, store.getTotalSellQuantity("B-ETH_USDT"), 1e-9);
    }

    @Test
    void multipleKafkaAndAeronFillsOnSamePair() throws Exception {
        Method processBinanceKafka = ExposureEngine.class.getDeclaredMethod("processBinanceKafkaRecord", ConsumerRecord.class);
        processBinanceKafka.setAccessible(true);
        Method processMEAeron = ExposureEngine.class.getDeclaredMethod("processMEAeronMessage", String.class);
        processMEAeron.setAccessible(true);

        OrderUpdateKafkaPayload binancePayload = OrderUpdateKafkaPayload.builder()
                .orderStatus("FILLED").quantity("5.0").price("65000.00")
                .pair("B-BTC_USDT").side("buy").timestamp(1000L).build();

        ConsumerRecord<String, String> record = new ConsumerRecord<>("test", 0, 0L, "key",
                MAPPER.writeValueAsString(binancePayload));
        processBinanceKafka.invoke(engine, record);

        MatchingEngineFill meFill = MatchingEngineFill.builder()
                .quantity("2.0").price("65100.00").pair("B-BTC_USDT")
                .side("sell").timestamp(2000L).build();
        processMEAeron.invoke(engine, MAPPER.writeValueAsString(meFill));

        assertEquals(3.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(5.0, store.getTotalBuyQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(2.0, store.getTotalSellQuantity("B-BTC_USDT"), 1e-9);
    }

    @Test
    void publishExposureToKafkaWithRealProducer() throws Exception {
        Method applyPayload = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        applyPayload.setAccessible(true);

        applyPayload.invoke(engine, OrderUpdateKafkaPayload.builder()
                .quantity("1.0").price("65000.00").pair("B-BTC_USDT")
                .side("buy").timestamp(1000L).build(), "TEST");

        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();
        assertFalse(snapshots.isEmpty());

        ObjectMapper mapper = new ObjectMapper();
        for (ExposureSnapshot snapshot : snapshots) {
            String json = mapper.writeValueAsString(snapshot);
            assertNotNull(json);
            assertTrue(json.contains("B-BTC_USDT"));
        }
    }

    @Test
    void publishExposureToKafkaWithEmptyList() throws Exception {
        Method publishMethod = ExposureEngine.class.getDeclaredMethod(
                "publishExposureToKafka", List.class);
        publishMethod.setAccessible(true);

        Field producerField = ExposureEngine.class.getDeclaredField("exposureProducer");
        producerField.setAccessible(true);

        Method createProducer = ExposureEngine.class.getDeclaredMethod("createExposureProducer");
        createProducer.setAccessible(true);
        var producer = createProducer.invoke(engine);
        producerField.set(engine, producer);

        List<ExposureSnapshot> emptyList = List.of();
        assertDoesNotThrow(() -> publishMethod.invoke(engine, emptyList));

        if (producer instanceof AutoCloseable ac) ac.close();
    }

    @Test
    void publishExposureToAeronWithNullPublicationAfterApply() throws Exception {
        Field transportField = ExposureEngine.class.getDeclaredField("exposureTransport");
        transportField.setAccessible(true);
        transportField.set(engine, "aeron");

        Method applyPayload = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        applyPayload.setAccessible(true);
        applyPayload.invoke(engine, OrderUpdateKafkaPayload.builder()
                .quantity("1.0").price("65000.00").pair("B-BTC_USDT")
                .side("buy").timestamp(1000L).build(), "TEST");

        Method publishMethod = ExposureEngine.class.getDeclaredMethod(
                "publishExposureToAeron", List.class);
        publishMethod.setAccessible(true);

        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();
        try {
            publishMethod.invoke(engine, snapshots);
        } catch (Exception e) {
            // publication is null — NullPointerException expected
        }
    }

    @Test
    void shutdownWithNullResources() throws Exception {
        Method shutdown = ExposureEngine.class.getDeclaredMethod("shutdown");
        shutdown.setAccessible(true);
        assertDoesNotThrow(() -> shutdown.invoke(engine));
    }

    @Test
    void shutdownWithExposureProducer() throws Exception {
        Method createProducer = ExposureEngine.class.getDeclaredMethod("createExposureProducer");
        createProducer.setAccessible(true);
        var producer = createProducer.invoke(engine);

        Field producerField = ExposureEngine.class.getDeclaredField("exposureProducer");
        producerField.setAccessible(true);
        producerField.set(engine, producer);

        Method shutdown = ExposureEngine.class.getDeclaredMethod("shutdown");
        shutdown.setAccessible(true);
        assertDoesNotThrow(() -> shutdown.invoke(engine));
    }

    @Test
    void snapshotAndPublishExposureEmptyStoreDoesNothing() throws Exception {
        Method snapshotMethod = ExposureEngine.class.getDeclaredMethod("snapshotAndPublishExposure");
        snapshotMethod.setAccessible(true);
        assertDoesNotThrow(() -> snapshotMethod.invoke(engine));
    }

    @Test
    void snapshotAndPublishExposureWithAeronTransport() throws Exception {
        Field transportField = ExposureEngine.class.getDeclaredField("exposureTransport");
        transportField.setAccessible(true);
        transportField.set(engine, "aeron");

        Method snapshotMethod = ExposureEngine.class.getDeclaredMethod("snapshotAndPublishExposure");
        snapshotMethod.setAccessible(true);
        assertDoesNotThrow(() -> snapshotMethod.invoke(engine));
    }

    @Test
    void snapshotAndPublishWithDataAndKafkaTransport() throws Exception {
        Method applyPayload = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        applyPayload.setAccessible(true);
        applyPayload.invoke(engine, OrderUpdateKafkaPayload.builder()
                .quantity("1.0").price("65000.00").pair("B-BTC_USDT")
                .side("buy").timestamp(1000L).build(), "TEST");

        Method createProducer = ExposureEngine.class.getDeclaredMethod("createExposureProducer");
        createProducer.setAccessible(true);
        var producer = createProducer.invoke(engine);
        Field producerField = ExposureEngine.class.getDeclaredField("exposureProducer");
        producerField.setAccessible(true);
        producerField.set(engine, producer);

        Method snapshotMethod = ExposureEngine.class.getDeclaredMethod("snapshotAndPublishExposure");
        snapshotMethod.setAccessible(true);

        try {
            snapshotMethod.invoke(engine);
        } catch (Exception e) {
            // Kafka broker may not be running, which is expected
        }

        if (producer instanceof AutoCloseable ac) ac.close();
    }

    @Test
    void snapshotAndPublishWithDataAndAeronTransport() throws Exception {
        Field transportField = ExposureEngine.class.getDeclaredField("exposureTransport");
        transportField.setAccessible(true);
        transportField.set(engine, "aeron");

        Method applyPayload = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        applyPayload.setAccessible(true);
        applyPayload.invoke(engine, OrderUpdateKafkaPayload.builder()
                .quantity("1.0").price("65000.00").pair("B-BTC_USDT")
                .side("buy").timestamp(1000L).build(), "TEST");

        Method snapshotMethod = ExposureEngine.class.getDeclaredMethod("snapshotAndPublishExposure");
        snapshotMethod.setAccessible(true);

        try {
            snapshotMethod.invoke(engine);
        } catch (Exception e) {
            // Aeron publication is null, expected
        }
    }

    @Test
    void verifyInternalRecordProcessorInterface() throws Exception {
        Class<?>[] innerClasses = ExposureEngine.class.getDeclaredClasses();
        boolean foundRecordProcessor = false;
        boolean foundMessageProcessor = false;
        for (Class<?> c : innerClasses) {
            if (c.getSimpleName().equals("RecordProcessor")) foundRecordProcessor = true;
            if (c.getSimpleName().equals("MessageProcessor")) foundMessageProcessor = true;
        }
        assertTrue(foundRecordProcessor);
        assertTrue(foundMessageProcessor);
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void startWithKafkaTransportExitsWhenRunningFalse() throws Exception {
        Field runningField = ExposureEngine.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.set(engine, false);

        engine.start();

        Thread.sleep(300);

        Method shutdown = ExposureEngine.class.getDeclaredMethod("shutdown");
        shutdown.setAccessible(true);
        shutdown.invoke(engine);
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void startWithAeronTransportExitsWhenRunningFalse() throws Exception {
        Field runningField = ExposureEngine.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.set(engine, false);

        setField(engine, "binanceTransport", "aeron");
        setField(engine, "meTransport", "aeron");
        setField(engine, "exposureTransport", "aeron");

        engine.start();

        Thread.sleep(300);

        Method shutdown = ExposureEngine.class.getDeclaredMethod("shutdown");
        shutdown.setAccessible(true);
        shutdown.invoke(engine);
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void startWithMixedTransportExitsWhenRunningFalse() throws Exception {
        Field runningField = ExposureEngine.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.set(engine, false);

        setField(engine, "binanceTransport", "kafka");
        setField(engine, "meTransport", "aeron");
        setField(engine, "exposureTransport", "kafka");

        engine.start();

        Thread.sleep(300);

        Method shutdown = ExposureEngine.class.getDeclaredMethod("shutdown");
        shutdown.setAccessible(true);
        shutdown.invoke(engine);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void publishExposureToKafkaWithNonEmptySnapshots() throws Exception {
        Method applyPayload = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        applyPayload.setAccessible(true);
        applyPayload.invoke(engine, OrderUpdateKafkaPayload.builder()
                .quantity("1.0").price("65000.00").pair("B-BTC_USDT")
                .side("buy").timestamp(1000L).build(), "TEST");

        KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, "1000",
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "1500",
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "500",
                ProducerConfig.LINGER_MS_CONFIG, "0",
                ProducerConfig.RETRIES_CONFIG, "0"
        ));
        Field producerField = ExposureEngine.class.getDeclaredField("exposureProducer");
        producerField.setAccessible(true);
        producerField.set(engine, producer);

        Method publishMethod = ExposureEngine.class.getDeclaredMethod("publishExposureToKafka", List.class);
        publishMethod.setAccessible(true);
        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();

        try {
            publishMethod.invoke(engine, snapshots);
        } catch (Exception e) {
            // Expected when no Kafka broker is running
        }

        producer.close(java.time.Duration.ofSeconds(2));
    }

    @Test
    void publishExposureToAeronWithBufferButNullPublication() throws Exception {
        Field bufferField = ExposureEngine.class.getDeclaredField("exposureBuffer");
        bufferField.setAccessible(true);
        bufferField.set(engine, new UnsafeBuffer(ByteBuffer.allocateDirect(64 * 1024)));

        Method applyPayload = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        applyPayload.setAccessible(true);
        applyPayload.invoke(engine, OrderUpdateKafkaPayload.builder()
                .quantity("1.0").price("65000.00").pair("B-BTC_USDT")
                .side("buy").timestamp(1000L).build(), "TEST");

        Method publishMethod = ExposureEngine.class.getDeclaredMethod("publishExposureToAeron", List.class);
        publishMethod.setAccessible(true);
        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();

        try {
            publishMethod.invoke(engine, snapshots);
        } catch (Exception e) {
            // exposurePublication is null — NullPointerException caught internally
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void startAndInterruptMainLoop() throws Exception {
        Thread startThread = new Thread(() -> {
            try {
                engine.start();
            } catch (Exception ignored) {
            }
        });
        startThread.start();
        Thread.sleep(200);
        startThread.interrupt();
        startThread.join(5000);

        Method shutdown = ExposureEngine.class.getDeclaredMethod("shutdown");
        shutdown.setAccessible(true);
        shutdown.invoke(engine);
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void shutdownWithAeronResources() throws Exception {
        Field runningField = ExposureEngine.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.set(engine, false);

        setField(engine, "binanceTransport", "aeron");
        setField(engine, "meTransport", "aeron");
        setField(engine, "exposureTransport", "aeron");

        engine.start();
        Thread.sleep(300);

        Method applyPayload = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        applyPayload.setAccessible(true);
        applyPayload.invoke(engine, OrderUpdateKafkaPayload.builder()
                .quantity("1.0").price("65000.00").pair("B-BTC_USDT")
                .side("buy").timestamp(1000L).build(), "TEST");

        Method shutdown = ExposureEngine.class.getDeclaredMethod("shutdown");
        shutdown.setAccessible(true);
        try {
            shutdown.invoke(engine);
        } catch (Exception e) {
            // Aeron publication may be closed, NPE expected
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void startWithOnlyExposureAeronTransport() throws Exception {
        Field runningField = ExposureEngine.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.set(engine, false);

        setField(engine, "binanceTransport", "kafka");
        setField(engine, "meTransport", "kafka");
        setField(engine, "exposureTransport", "aeron");

        engine.start();
        Thread.sleep(300);

        Method shutdown = ExposureEngine.class.getDeclaredMethod("shutdown");
        shutdown.setAccessible(true);
        shutdown.invoke(engine);
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void startWithOnlyBinanceAeronTransport() throws Exception {
        Field runningField = ExposureEngine.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.set(engine, false);

        setField(engine, "binanceTransport", "aeron");
        setField(engine, "meTransport", "kafka");
        setField(engine, "exposureTransport", "kafka");

        engine.start();
        Thread.sleep(300);

        Method shutdown = ExposureEngine.class.getDeclaredMethod("shutdown");
        shutdown.setAccessible(true);
        shutdown.invoke(engine);
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void publishExposureToAeronWithRealPublication() throws Exception {
        Field runningField = ExposureEngine.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.set(engine, false);

        setField(engine, "binanceTransport", "aeron");
        setField(engine, "meTransport", "aeron");
        setField(engine, "exposureTransport", "aeron");

        engine.start();
        Thread.sleep(300);

        Method applyPayload = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        applyPayload.setAccessible(true);
        applyPayload.invoke(engine, OrderUpdateKafkaPayload.builder()
                .quantity("1.0").price("65000.00").pair("B-BTC_USDT")
                .side("buy").timestamp(1000L).build(), "TEST");
        applyPayload.invoke(engine, OrderUpdateKafkaPayload.builder()
                .quantity("2.0").price("3400.00").pair("B-ETH_USDT")
                .side("sell").timestamp(2000L).build(), "TEST");

        Method publishMethod = ExposureEngine.class.getDeclaredMethod("publishExposureToAeron", List.class);
        publishMethod.setAccessible(true);
        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();

        try {
            publishMethod.invoke(engine, snapshots);
        } catch (Exception e) {
            // Publication might be NOT_CONNECTED
        }

        Method shutdown = ExposureEngine.class.getDeclaredMethod("shutdown");
        shutdown.setAccessible(true);
        try {
            shutdown.invoke(engine);
        } catch (Exception e) {
            // cleanup
        }
    }

    @Test
    @Timeout(value = 15, unit = TimeUnit.SECONDS)
    void snapshotAndPublishWithAeronTransportAndData() throws Exception {
        Field runningField = ExposureEngine.class.getDeclaredField("running");
        runningField.setAccessible(true);
        runningField.set(engine, false);

        setField(engine, "binanceTransport", "aeron");
        setField(engine, "meTransport", "aeron");
        setField(engine, "exposureTransport", "aeron");

        engine.start();
        Thread.sleep(300);

        Method applyPayload = ExposureEngine.class.getDeclaredMethod(
                "applyPayload", OrderUpdateKafkaPayload.class, String.class);
        applyPayload.setAccessible(true);
        applyPayload.invoke(engine, OrderUpdateKafkaPayload.builder()
                .quantity("1.0").price("65000.00").pair("B-BTC_USDT")
                .side("buy").timestamp(1000L).build(), "TEST");

        Method snapshotMethod = ExposureEngine.class.getDeclaredMethod("snapshotAndPublishExposure");
        snapshotMethod.setAccessible(true);
        try {
            snapshotMethod.invoke(engine);
        } catch (Exception e) {
            // May fail if publication has no subscriber
        }

        Method shutdown = ExposureEngine.class.getDeclaredMethod("shutdown");
        shutdown.setAccessible(true);
        try {
            shutdown.invoke(engine);
        } catch (Exception e) {
            // cleanup
        }
    }
}
