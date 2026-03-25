package com.github.programmingwithmati.exposureEngine.testPublishers.aeron;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateKafkaPayload;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Test Aeron publisher that simulates Binance order-update events over Aeron.
 * Run ExposureEngine (with BINANCE_TRANSPORT=aeron) first, then run this.
 *
 * Env vars:
 *   BINANCE_AERON_CHANNEL   – (default: aeron:udp?endpoint=localhost:40457)
 *   BINANCE_AERON_STREAM_ID – (default: 30)
 */
public class BinanceTestAeronPublisher {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final String channel = envOrDefault("BINANCE_AERON_CHANNEL", "aeron:udp?endpoint=localhost:40457");
        final int streamId = Integer.parseInt(envOrDefault("BINANCE_AERON_STREAM_ID", "30"));

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        try (MediaDriver driver = MediaDriver.launchEmbedded(driverCtx);
             Aeron aeron = Aeron.connect(new Aeron.Context()
                     .aeronDirectoryName(driver.aeronDirectoryName()));
             Publication publication = aeron.addPublication(channel, streamId)) {

            System.out.println("BinanceTestAeronPublisher on " + channel + " stream=" + streamId);
            waitForSubscriber(publication);

            long now = System.currentTimeMillis();

            List<OrderUpdateKafkaPayload> events = List.of(
                    OrderUpdateKafkaPayload.builder()
                            .derivativesFuturesOrderId("BN-ORD-201")
                            .quantity("2.0")
                            .price("65050.00")
                            .pair("B-BTC_USDT")
                            .exchangeTradeId(80001)
                            .ecode("B")
                            .orderStatus("FILLED")
                            .timestamp(now)
                            .orderFilledQuantity("2.0")
                            .orderAvgPrice("65050.00")
                            .side("buy")
                            .isMaker(false)
                            .build(),
                    OrderUpdateKafkaPayload.builder()
                            .derivativesFuturesOrderId("BN-ORD-202")
                            .quantity("15.0")
                            .price("3400.00")
                            .pair("B-ETH_USDT")
                            .exchangeTradeId(80002)
                            .ecode("B")
                            .orderStatus("FILLED")
                            .timestamp(now + 1)
                            .orderFilledQuantity("15.0")
                            .orderAvgPrice("3400.00")
                            .side("sell")
                            .isMaker(true)
                            .build(),
                    OrderUpdateKafkaPayload.builder()
                            .derivativesFuturesOrderId("BN-ORD-203")
                            .quantity("0.5")
                            .price("65200.00")
                            .pair("B-BTC_USDT")
                            .exchangeTradeId(80003)
                            .ecode("B")
                            .orderStatus("PARTIALLY_FILLED")
                            .timestamp(now + 2)
                            .orderFilledQuantity("0.5")
                            .orderAvgPrice("65200.00")
                            .side("sell")
                            .isMaker(false)
                            .build(),
                    OrderUpdateKafkaPayload.builder()
                            .derivativesFuturesOrderId("BN-ORD-204")
                            .quantity("500.0")
                            .price("0.1590")
                            .pair("B-DOGE_USDT")
                            .exchangeTradeId(80004)
                            .ecode("B")
                            .orderStatus("FILLED")
                            .timestamp(now + 3)
                            .orderFilledQuantity("500.0")
                            .orderAvgPrice("0.1590")
                            .side("sell")
                            .isMaker(true)
                            .build(),
                    OrderUpdateKafkaPayload.builder()
                            .derivativesFuturesOrderId("BN-ORD-205")
                            .quantity("30.0")
                            .price("143.50")
                            .pair("B-LTC_USDT")
                            .exchangeTradeId(80005)
                            .ecode("B")
                            .orderStatus("FILLED")
                            .timestamp(now + 4)
                            .orderFilledQuantity("30.0")
                            .orderAvgPrice("143.50")
                            .side("buy")
                            .isMaker(false)
                            .build()
            );

            UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));

            for (OrderUpdateKafkaPayload event : events) {
                byte[] jsonBytes = OBJECT_MAPPER.writeValueAsBytes(event);
                buffer.putBytes(0, jsonBytes);

                long result;
                while ((result = publication.offer(buffer, 0, jsonBytes.length)) < 0L) {
                    if (result == Publication.CLOSED) {
                        System.err.println("Publication closed, aborting.");
                        return;
                    }
                    Thread.yield();
                }

                System.out.printf("Sent | pair=%s side=%s qty=%s price=%s status=%s%n",
                        event.getPair(), event.getSide(), event.getQuantity(),
                        event.getPrice(), event.getOrderStatus());

                Thread.sleep(500);
            }

            System.out.println("All " + events.size() + " Binance test events sent via Aeron.");
        }
    }

    private static void waitForSubscriber(Publication publication) throws InterruptedException {
        System.out.println("Waiting for subscriber to connect...");
        final long deadline = System.currentTimeMillis() + 10_000;
        while (!publication.isConnected()) {
            if (System.currentTimeMillis() > deadline) {
                System.err.println("Timed out. Is ExposureEngine (BINANCE_TRANSPORT=aeron) running?");
                System.exit(1);
            }
            Thread.sleep(100);
        }
        System.out.println("Subscriber connected.");
    }

    private static String envOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
