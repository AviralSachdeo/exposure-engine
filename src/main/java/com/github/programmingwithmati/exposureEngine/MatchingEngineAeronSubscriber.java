package com.github.programmingwithmati.exposureEngine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.MatchingEngineFill;
import io.aeron.Aeron;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;

/**
 * Aeron subscriber that listens for matching-engine fill events.
 *
 * The matching engine publishes JSON-encoded {@link MatchingEngineFill} messages
 * over Aeron IPC/UDP. This subscriber deserializes them and hands them off to
 * {@link #processFill(MatchingEngineFill)} for downstream processing
 * (exposure state updates, risk calculations, etc.).
 *
 * Env vars:
 *   ME_AERON_CHANNEL   – Aeron channel (default: aeron:udp?endpoint=localhost:40456)
 *   ME_AERON_STREAM_ID – Aeron stream id  (default: 20)
 */
public class MatchingEngineAeronSubscriber {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final String channel;
    private final int streamId;
    private volatile boolean running = true;

    public MatchingEngineAeronSubscriber() {
        this.channel = envOrDefault("ME_AERON_CHANNEL", "aeron:udp?endpoint=localhost:40456");
        this.streamId = Integer.parseInt(envOrDefault("ME_AERON_STREAM_ID", "20"));
    }

    public static void main(String[] args) {
        MatchingEngineAeronSubscriber subscriber = new MatchingEngineAeronSubscriber();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> subscriber.running = false));
        subscriber.start();
    }

    public void start() {
        final MediaDriver.Context driverCtx = new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);

        try (MediaDriver driver = MediaDriver.launchEmbedded(driverCtx);
             Aeron aeron = Aeron.connect(new Aeron.Context()
                     .aeronDirectoryName(driver.aeronDirectoryName()));
             Subscription subscription = aeron.addSubscription(channel, streamId)) {

            System.out.println("MatchingEngineAeronSubscriber started on " + channel +
                    " streamId=" + streamId);
            System.out.println("Waiting for matching-engine fill events...");

            final FragmentHandler fragmentHandler = (buffer, offset, length, header) -> {
                byte[] bytes = new byte[length];
                buffer.getBytes(offset, bytes);
                handleMessage(new String(bytes));
            };

            while (running) {
                int fragmentsRead = subscription.poll(fragmentHandler, 10);
                if (fragmentsRead == 0) {
                    Thread.sleep(1);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Subscriber interrupted.");
        } catch (Exception e) {
            System.err.println("Subscriber error: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("MatchingEngineAeronSubscriber stopped.");
    }

    private void handleMessage(String json) {
        try {
            MatchingEngineFill fill = OBJECT_MAPPER.readValue(json, MatchingEngineFill.class);

            System.out.printf("ME FILL | pair=%s side=%s qty=%s price=%s orderId=%s tradeId=%s isMaker=%s ts=%d%n",
                    fill.getPair(), fill.getSide(), fill.getQuantity(), fill.getPrice(),
                    fill.getDerivativesFuturesOrderId(), fill.getExchangeTradeId(),
                    fill.isMaker(), fill.getTimestamp());

            processFill(fill);
        } catch (Exception e) {
            System.err.println("Error deserializing fill: " + e.getMessage());
            System.err.println("Raw: " + json.substring(0, Math.min(200, json.length())));
        }
    }

    /**
     * Extension point: update off-heap exposure state, compute risk, publish risk updates.
     */
    private void processFill(MatchingEngineFill fill) {
        // TODO: plug in ExposureStateStore and RiskCalculator
    }

    private static String envOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
