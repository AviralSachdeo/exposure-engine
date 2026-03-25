package com.github.programmingwithmati.exposureEngine.testPublishers.aeron;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.MatchingEngineFill;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Test publisher that simulates matching-engine fill events over Aeron.
 * Run ExposureEngine (with ME_TRANSPORT=aeron) first, then run this to send sample fills.
 *
 * Env vars:
 *   ME_AERON_CHANNEL   – Aeron channel (default: aeron:udp?endpoint=localhost:40456)
 *   ME_AERON_STREAM_ID – Aeron stream id  (default: 20)
 */
public class MatchingEngineTestPublisher {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        final String channel = envOrDefault("ME_AERON_CHANNEL", "aeron:udp?endpoint=localhost:40456");
        final int streamId = Integer.parseInt(envOrDefault("ME_AERON_STREAM_ID", "20"));

        final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        try (MediaDriver driver = MediaDriver.launchEmbedded(driverCtx);
             Aeron aeron = Aeron.connect(new Aeron.Context()
                     .aeronDirectoryName(driver.aeronDirectoryName()));
             Publication publication = aeron.addPublication(channel, streamId)) {

            System.out.println("MatchingEngineTestPublisher on " + channel + " streamId=" + streamId);
            waitForSubscriber(publication);

            long now = System.currentTimeMillis();

            List<MatchingEngineFill> fills = List.of(
                    MatchingEngineFill.builder()
                            .derivativesFuturesOrderId("ME-ORD-001")
                            .quantity("0.5")
                            .price("65200.50")
                            .pair("B-BTC_USDT")
                            .side("buy")
                            .exchangeTradeId("ME-TRD-1001")
                            .timestamp(now)
                            .isMaker(false)
                            .build(),
                    MatchingEngineFill.builder()
                            .derivativesFuturesOrderId("ME-ORD-002")
                            .quantity("10.0")
                            .price("3410.25")
                            .pair("B-ETH_USDT")
                            .side("sell")
                            .exchangeTradeId("ME-TRD-1002")
                            .timestamp(now + 1)
                            .isMaker(true)
                            .build(),
                    MatchingEngineFill.builder()
                            .derivativesFuturesOrderId("ME-ORD-003")
                            .quantity("100.0")
                            .price("142.80")
                            .pair("B-LTC_USDT")
                            .side("buy")
                            .exchangeTradeId("ME-TRD-1003")
                            .timestamp(now + 2)
                            .isMaker(false)
                            .build(),
                    MatchingEngineFill.builder()
                            .derivativesFuturesOrderId("ME-ORD-004")
                            .quantity("5000.0")
                            .price("0.1623")
                            .pair("B-DOGE_USDT")
                            .side("sell")
                            .exchangeTradeId("ME-TRD-1004")
                            .timestamp(now + 3)
                            .isMaker(true)
                            .build(),
                    MatchingEngineFill.builder()
                            .derivativesFuturesOrderId("ME-ORD-005")
                            .quantity("2.5")
                            .price("65180.00")
                            .pair("B-BTC_USDT")
                            .side("buy")
                            .exchangeTradeId("ME-TRD-1005")
                            .timestamp(now + 4)
                            .isMaker(false)
                            .build()
            );

            UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(4096));

            for (MatchingEngineFill fill : fills) {
                byte[] jsonBytes = OBJECT_MAPPER.writeValueAsBytes(fill);
                buffer.putBytes(0, jsonBytes);

                long result;
                while ((result = publication.offer(buffer, 0, jsonBytes.length)) < 0L) {
                    if (result == Publication.CLOSED) {
                        System.err.println("Publication closed, aborting.");
                        return;
                    }
                    Thread.yield();
                }

                System.out.printf("Sent | pair=%s side=%s qty=%s price=%s orderId=%s%n",
                        fill.getPair(), fill.getSide(), fill.getQuantity(), fill.getPrice(),
                        fill.getDerivativesFuturesOrderId());

                Thread.sleep(500);
            }

            System.out.println("All " + fills.size() + " test fills sent.");
        }
    }

    private static void waitForSubscriber(Publication publication) throws InterruptedException {
        System.out.println("Waiting for subscriber to connect...");
        final long deadline = System.currentTimeMillis() + 10_000;
        while (!publication.isConnected()) {
            if (System.currentTimeMillis() > deadline) {
                System.err.println("Timed out. Is MatchingEngineAeronSubscriber running?");
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
