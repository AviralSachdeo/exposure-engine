package com.github.programmingwithmati.exposureEngine.loadTestPublishers.aeron;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.MatchingEngineFill;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateKafkaPayload;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * Aeron load-test publisher: sends 10,000 messages for a single pair
 * split across Binance and ME Aeron channels (5,000 each).
 *
 * Alternates buy/sell, varies quantity and price slightly per message.
 * Prints throughput stats on completion.
 *
 * Env vars:
 *   BINANCE_AERON_CHANNEL   – (default: aeron:udp?endpoint=localhost:40457)
 *   BINANCE_AERON_STREAM_ID – (default: 30)
 *   ME_AERON_CHANNEL        – (default: aeron:udp?endpoint=localhost:40456)
 *   ME_AERON_STREAM_ID      – (default: 20)
 *   LOAD_TEST_PAIR          – instrument pair to use (default: B-BTC_USDT)
 *   LOAD_TEST_TOTAL         – total message count    (default: 10000)
 */
public class LoadTestAeronPublisher {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String binanceChannel = envOrDefault("BINANCE_AERON_CHANNEL", "aeron:udp?endpoint=localhost:40457");
        int binanceStreamId = Integer.parseInt(envOrDefault("BINANCE_AERON_STREAM_ID", "30"));
        String meChannel = envOrDefault("ME_AERON_CHANNEL", "aeron:udp?endpoint=localhost:40456");
        int meStreamId = Integer.parseInt(envOrDefault("ME_AERON_STREAM_ID", "20"));
        String pair = envOrDefault("LOAD_TEST_PAIR", "B-BTC_USDT");
        int totalMessages = Integer.parseInt(envOrDefault("LOAD_TEST_TOTAL", "10000"));

        int binanceCount = totalMessages / 2;
        int meCount = totalMessages - binanceCount;

        System.out.println("╔════════════════════════════════════════╗");
        System.out.println("║     AERON LOAD TEST PUBLISHER          ║");
        System.out.println("╠════════════════════════════════════════╣");
        System.out.printf("║  Pair         : %-23s║%n", pair);
        System.out.printf("║  Binance msgs : %-23d║%n", binanceCount);
        System.out.printf("║  ME msgs      : %-23d║%n", meCount);
        System.out.printf("║  Total        : %-23d║%n", totalMessages);
        System.out.println("╚════════════════════════════════════════╝");

        MediaDriver.Context driverCtx = new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        try (MediaDriver driver = MediaDriver.launchEmbedded(driverCtx);
             Aeron aeron = Aeron.connect(new Aeron.Context()
                     .aeronDirectoryName(driver.aeronDirectoryName()));
             Publication binancePub = aeron.addPublication(binanceChannel, binanceStreamId);
             Publication mePub = aeron.addPublication(meChannel, meStreamId)) {

            System.out.println("\nBinance pub: " + binanceChannel + " stream=" + binanceStreamId);
            System.out.println("ME pub     : " + meChannel + " stream=" + meStreamId);

            System.out.println("\nWaiting for ExposureEngine subscribers...");
            waitForSubscriber(binancePub, "Binance");
            waitForSubscriber(mePub, "ME");
            System.out.println("Both subscribers connected.\n");

            UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(64 * 1024));

            long baseTs = System.currentTimeMillis();
            double basePrice = 65000.0;
            double baseQty = 0.1;

            int sentCount = 0;
            int backPressureCount = 0;

            long startNs = System.nanoTime();

            System.out.println("Sending " + binanceCount + " Binance order updates...");
            for (int i = 0; i < binanceCount; i++) {
                String side = (i % 2 == 0) ? "buy" : "sell";
                double price = basePrice + (i % 100) * 10.0;
                double qty = baseQty + (i % 50) * 0.01;

                OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.builder()
                        .derivativesFuturesOrderId("LT-BN-" + i)
                        .quantity(String.format("%.4f", qty))
                        .price(String.format("%.2f", price))
                        .pair(pair)
                        .exchangeTradeId(200000 + i)
                        .ecode("B")
                        .orderStatus("FILLED")
                        .timestamp(baseTs + i)
                        .orderFilledQuantity(String.format("%.4f", qty))
                        .orderAvgPrice(String.format("%.2f", price))
                        .side(side)
                        .isMaker(i % 3 == 0)
                        .build();

                byte[] jsonBytes = OBJECT_MAPPER.writeValueAsBytes(payload);
                buffer.putBytes(0, jsonBytes);

                long result;
                while ((result = binancePub.offer(buffer, 0, jsonBytes.length)) < 0L) {
                    if (result == Publication.CLOSED) {
                        System.err.println("Binance publication closed, aborting.");
                        return;
                    }
                    backPressureCount++;
                    Thread.yield();
                }
                sentCount++;

                if (sentCount % 1000 == 0) {
                    System.out.printf("  [Binance] %,d / %,d sent%n", sentCount, binanceCount);
                }
            }

            int meSent = 0;
            System.out.println("Sending " + meCount + " ME fill events...");
            for (int i = 0; i < meCount; i++) {
                String side = (i % 2 == 0) ? "buy" : "sell";
                double price = basePrice + (i % 100) * 10.0;
                double qty = baseQty + (i % 50) * 0.01;

                MatchingEngineFill fill = MatchingEngineFill.builder()
                        .derivativesFuturesOrderId("LT-ME-" + i)
                        .quantity(String.format("%.4f", qty))
                        .price(String.format("%.2f", price))
                        .pair(pair)
                        .side(side)
                        .exchangeTradeId("LT-TRD-" + i)
                        .timestamp(baseTs + binanceCount + i)
                        .isMaker(i % 3 == 0)
                        .build();

                byte[] jsonBytes = OBJECT_MAPPER.writeValueAsBytes(fill);
                buffer.putBytes(0, jsonBytes);

                long result;
                while ((result = mePub.offer(buffer, 0, jsonBytes.length)) < 0L) {
                    if (result == Publication.CLOSED) {
                        System.err.println("ME publication closed, aborting.");
                        return;
                    }
                    backPressureCount++;
                    Thread.yield();
                }
                sentCount++;
                meSent++;

                if (meSent % 1000 == 0) {
                    System.out.printf("  [ME]      %,d / %,d sent%n", meSent, meCount);
                }
            }

            long elapsedNs = System.nanoTime() - startNs;
            double elapsedMs = elapsedNs / 1_000_000.0;
            double elapsedSec = elapsedMs / 1_000.0;
            double throughput = totalMessages / elapsedSec;

            System.out.println("\n╔════════════════════════════════════════╗");
            System.out.println("║           LOAD TEST RESULTS            ║");
            System.out.println("╠════════════════════════════════════════╣");
            System.out.printf("║  Total sent    : %,d msgs %12s║%n", sentCount, "");
            System.out.printf("║  Back-pressure : %,d retries %9s║%n", backPressureCount, "");
            System.out.printf("║  Elapsed       : %,.2f ms %13s║%n", elapsedMs, "");
            System.out.printf("║  Throughput    : %,.0f msgs/sec %7s║%n", throughput, "");
            System.out.println("╚════════════════════════════════════════╝");

            Thread.sleep(2000);
        }
    }

    private static void waitForSubscriber(Publication publication, String label) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 15_000;
        while (!publication.isConnected()) {
            if (System.currentTimeMillis() > deadline) {
                System.err.println("[" + label + "] Timed out waiting for subscriber. "
                        + "Is ExposureEngine running with the correct Aeron transport config?");
                System.exit(1);
            }
            Thread.sleep(100);
        }
        System.out.println("  [" + label + "] subscriber connected.");
    }

    private static String envOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
