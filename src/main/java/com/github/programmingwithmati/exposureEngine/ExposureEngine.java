package com.github.programmingwithmati.exposureEngine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.ExposureSnapshot;
import com.github.programmingwithmati.exposureEngine.model.MatchingEngineFill;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateKafkaPayload;
import com.github.programmingwithmati.exposureEngine.store.OffHeapExposureStore;
import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Joins two streams — Binance order updates and matching-engine fills —
 * into a single authoritative off-heap exposure state.
 *
 * Each stream's transport (Kafka or Aeron) is independently configurable:
 *
 * <h3>Transport config (env vars):</h3>
 * <pre>
 *   BINANCE_TRANSPORT       – "kafka" or "aeron"  (default: kafka)
 *   ME_TRANSPORT            – "kafka" or "aeron"  (default: aeron)
 *   EXPOSURE_TRANSPORT      – "kafka" or "aeron"  (default: kafka)
 * </pre>
 *
 * <h3>Kafka config:</h3>
 * <pre>
 *   KAFKA_BOOTSTRAP         – broker address       (default: localhost:9092)
 *   CONSUMER_GROUP          – consumer group id     (default: exposure-engine)
 *   BINANCE_KAFKA_TOPIC     – Binance order topic   (default: binance_order_updates)
 *   ME_KAFKA_TOPIC          – ME fills topic        (default: me_fills)
 *   EXPOSURE_KAFKA_TOPIC    – exposure output topic (default: exposure_snapshots)
 * </pre>
 *
 * <h3>Aeron config:</h3>
 * <pre>
 *   BINANCE_AERON_CHANNEL   – channel for Binance   (default: aeron:udp?endpoint=localhost:40457)
 *   BINANCE_AERON_STREAM_ID – stream id             (default: 30)
 *   ME_AERON_CHANNEL        – channel for ME fills     (default: aeron:udp?endpoint=localhost:40456)
 *   ME_AERON_STREAM_ID      – stream id               (default: 20)
 *   EXPOSURE_AERON_CHANNEL  – channel for exposure out (default: aeron:udp?endpoint=localhost:40458)
 *   EXPOSURE_AERON_STREAM_ID – stream id               (default: 40)
 * </pre>
 */
public class ExposureEngine {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final OffHeapExposureStore store;

    private final String binanceTransport;
    private final String meTransport;

    private final String kafkaBootstrap;
    private final String consumerGroup;
    private final String binanceKafkaTopic;
    private final String meKafkaTopic;

    private final String binanceAeronChannel;
    private final int binanceAeronStreamId;
    private final String meAeronChannel;
    private final int meAeronStreamId;

    private final String exposureTransport;
    private final String exposureKafkaTopic;
    private final String exposureAeronChannel;
    private final int exposureAeronStreamId;

    private volatile boolean running = true;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private MediaDriver sharedDriver;
    private Aeron sharedAeron;
    private KafkaProducer<String, String> exposureProducer;
    private Publication exposurePublication;
    private UnsafeBuffer exposureBuffer;

    public ExposureEngine() {
        this.store = new OffHeapExposureStore();

        this.binanceTransport = envOrDefault("BINANCE_TRANSPORT", "kafka").toLowerCase();
        this.meTransport = envOrDefault("ME_TRANSPORT", "kafka").toLowerCase();

        this.kafkaBootstrap = envOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        this.consumerGroup = envOrDefault("CONSUMER_GROUP", "exposure-engine");
        this.binanceKafkaTopic = envOrDefault("BINANCE_KAFKA_TOPIC", "binance_order_updates");
        this.meKafkaTopic = envOrDefault("ME_KAFKA_TOPIC", "me_fills");

        this.binanceAeronChannel = envOrDefault("BINANCE_AERON_CHANNEL", "aeron:udp?endpoint=localhost:40457");
        this.binanceAeronStreamId = Integer.parseInt(envOrDefault("BINANCE_AERON_STREAM_ID", "30"));
        this.meAeronChannel = envOrDefault("ME_AERON_CHANNEL", "aeron:udp?endpoint=localhost:40456");
        this.meAeronStreamId = Integer.parseInt(envOrDefault("ME_AERON_STREAM_ID", "20"));

        this.exposureTransport = envOrDefault("EXPOSURE_TRANSPORT", "kafka").toLowerCase();
        this.exposureKafkaTopic = envOrDefault("EXPOSURE_KAFKA_TOPIC", "exposure_snapshots");
        this.exposureAeronChannel = envOrDefault("EXPOSURE_AERON_CHANNEL", "aeron:udp?endpoint=localhost:40458");
        this.exposureAeronStreamId = Integer.parseInt(envOrDefault("EXPOSURE_AERON_STREAM_ID", "40"));
    }

    public static void main(String[] args) {
        ExposureEngine engine = new ExposureEngine();
        Runtime.getRuntime().addShutdownHook(new Thread(engine::shutdown));
        engine.start();
    }

    public void start() {
        System.out.println("╔═══════════════════════════════════╗");
        System.out.println("║     EXPOSURE ENGINE STARTING      ║");
        System.out.println("╚═══════════════════════════════════╝");

        boolean needsAeron = "aeron".equals(binanceTransport)
                || "aeron".equals(meTransport)
                || "aeron".equals(exposureTransport);
        if (needsAeron) {
            initSharedAeron();
        }

        if ("kafka".equals(exposureTransport)) {
            exposureProducer = createExposureProducer();
        } else {
            exposurePublication = sharedAeron.addPublication(exposureAeronChannel, exposureAeronStreamId);
            exposureBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(64 * 1024));
            System.out.println("[EXPOSURE] Aeron publication on " + exposureAeronChannel
                    + " stream=" + exposureAeronStreamId);
        }

        // ---- Binance stream ----
        if ("kafka".equals(binanceTransport)) {
            Thread t = new Thread(
                    () -> runKafkaConsumer(binanceKafkaTopic, this::processBinanceKafkaRecord, "BINANCE"),
                    "binance-kafka-consumer");
            t.setDaemon(true);
            t.start();
        } else {
            Thread t = new Thread(
                    () -> runAeronSubscriber(binanceAeronChannel, binanceAeronStreamId,
                            this::processBinanceAeronMessage, "BINANCE"),
                    "binance-aeron-subscriber");
            t.setDaemon(true);
            t.start();
        }

        // ---- Matching engine stream ----
        if ("kafka".equals(meTransport)) {
            Thread t = new Thread(
                    () -> runKafkaConsumer(meKafkaTopic, this::processMEKafkaRecord, "ME"),
                    "me-kafka-consumer");
            t.setDaemon(true);
            t.start();
        } else {
            Thread t = new Thread(
                    () -> runAeronSubscriber(meAeronChannel, meAeronStreamId,
                            this::processMEAeronMessage, "ME"),
                    "me-aeron-subscriber");
            t.setDaemon(true);
            t.start();
        }

        scheduler.scheduleAtFixedRate(this::snapshotAndPublishExposure, 5, 5, TimeUnit.SECONDS);

        System.out.println("ExposureEngine running:");
        System.out.println("  Binance stream : " + binanceTransport.toUpperCase()
                + ("kafka".equals(binanceTransport)
                ? " [" + binanceKafkaTopic + " @ " + kafkaBootstrap + "]"
                : " [" + binanceAeronChannel + " stream=" + binanceAeronStreamId + "]"));
        System.out.println("  ME stream      : " + meTransport.toUpperCase()
                + ("kafka".equals(meTransport)
                ? " [" + meKafkaTopic + " @ " + kafkaBootstrap + "]"
                : " [" + meAeronChannel + " stream=" + meAeronStreamId + "]"));
        System.out.println("  Exposure output: " + exposureTransport.toUpperCase()
                + ("kafka".equals(exposureTransport)
                ? " [" + exposureKafkaTopic + " @ " + kafkaBootstrap + "]"
                : " [" + exposureAeronChannel + " stream=" + exposureAeronStreamId + "]"));
        System.out.println("  Snapshot interval: every 5 seconds.\n");

        while (running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void initSharedAeron() {
        MediaDriver.Context driverCtx = new MediaDriver.Context()
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true);
        sharedDriver = MediaDriver.launchEmbedded(driverCtx);
        sharedAeron = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(sharedDriver.aeronDirectoryName()));
        System.out.println("[AERON] Shared MediaDriver started: " + sharedDriver.aeronDirectoryName());
    }

    @FunctionalInterface
    private interface RecordProcessor {
        void process(ConsumerRecord<String, String> record);
    }

    private void runKafkaConsumer(String topic, RecordProcessor processor, String label) {
        try (KafkaConsumer<String, String> consumer = createKafkaConsumer()) {
            consumer.subscribe(List.of(topic));
            System.out.println("[" + label + "] Kafka consumer subscribed to [" + topic + "]");

            while (running) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> record : records) {
                    processor.process(record);
                }
            }
        } catch (Exception e) {
            System.err.println("[" + label + "] Kafka consumer error: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("[" + label + "] Kafka consumer stopped.");
    }

    @FunctionalInterface
    private interface MessageProcessor {
        void process(String json);
    }

    private void runAeronSubscriber(String channel, int streamId,
                                    MessageProcessor processor, String label) {
        try (Subscription subscription = sharedAeron.addSubscription(channel, streamId)) {
            System.out.println("[" + label + "] Aeron subscriber started on "
                    + channel + " stream=" + streamId);

            final FragmentHandler handler = (buffer, offset, length, header) -> {
                byte[] bytes = new byte[length];
                buffer.getBytes(offset, bytes);
                processor.process(new String(bytes));
            };

            while (running) {
                int fragmentsRead = subscription.poll(handler, 10);
                if (fragmentsRead == 0) {
                    Thread.sleep(1);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.err.println("[" + label + "] Aeron subscriber error: " + e.getMessage());
            e.printStackTrace();
        }
        System.out.println("[" + label + "] Aeron subscriber stopped.");
    }

    private void processBinanceKafkaRecord(ConsumerRecord<String, String> record) {
        try {
            OrderUpdateKafkaPayload payload = OBJECT_MAPPER.readValue(
                    record.value(), OrderUpdateKafkaPayload.class);

            String status = payload.getOrderStatus();
            if (!"FILLED".equals(status) && !"PARTIALLY_FILLED".equals(status)) {
                return;
            }

            applyPayload(payload, "BINANCE-KAFKA");
        } catch (Exception e) {
            System.err.println("[BINANCE] Kafka error at offset " + record.offset() + ": " + e.getMessage());
        }
    }

    private void processBinanceAeronMessage(String json) {
        try {
            OrderUpdateKafkaPayload payload = OBJECT_MAPPER.readValue(json, OrderUpdateKafkaPayload.class);

            String status = payload.getOrderStatus();
            if (!"FILLED".equals(status) && !"PARTIALLY_FILLED".equals(status)) {
                return;
            }

            applyPayload(payload, "BINANCE-AERON");
        } catch (Exception e) {
            System.err.println("[BINANCE] Aeron error: " + e.getMessage());
        }
    }

    private void applyPayload(OrderUpdateKafkaPayload payload, String source) {
        double qty = Double.parseDouble(payload.getQuantity());
        double price = Double.parseDouble(payload.getPrice());
        store.applyFill(payload.getPair(), payload.getSide(), qty, price, payload.getTimestamp(), source);
    }

    private void processMEKafkaRecord(ConsumerRecord<String, String> record) {
        try {
            MatchingEngineFill fill = OBJECT_MAPPER.readValue(record.value(), MatchingEngineFill.class);
            applyFill(fill, "ME-KAFKA");
        } catch (Exception e) {
            System.err.println("[ME] Kafka error at offset " + record.offset() + ": " + e.getMessage());
        }
    }

    private void processMEAeronMessage(String json) {
        try {
            MatchingEngineFill fill = OBJECT_MAPPER.readValue(json, MatchingEngineFill.class);
            applyFill(fill, "ME-AERON");
        } catch (Exception e) {
            System.err.println("[ME] Aeron error: " + e.getMessage());
        }
    }

    private void applyFill(MatchingEngineFill fill, String source) {
        double qty = Double.parseDouble(fill.getQuantity());
        double price = Double.parseDouble(fill.getPrice());
        store.applyFill(fill.getPair(), fill.getSide(), qty, price, fill.getTimestamp(), source);
    }

    private void snapshotAndPublishExposure() {
        store.printExposureSnapshot();

        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();
        if (snapshots.isEmpty()) {
            return;
        }

        if ("kafka".equals(exposureTransport)) {
            publishExposureToKafka(snapshots);
        } else {
            publishExposureToAeron(snapshots);
        }
    }

    private void publishExposureToKafka(List<ExposureSnapshot> snapshots) {
        for (ExposureSnapshot snapshot : snapshots) {
            try {
                String value = OBJECT_MAPPER.writeValueAsString(snapshot);
                exposureProducer.send(
                        new ProducerRecord<>(exposureKafkaTopic, snapshot.getPair(), value),
                        (meta, ex) -> {
                            if (ex != null) {
                                System.err.println("[EXPOSURE] Kafka publish failed for "
                                        + snapshot.getPair() + ": " + ex.getMessage());
                            }
                        });
            } catch (Exception e) {
                System.err.println("[EXPOSURE] Error serializing snapshot for "
                        + snapshot.getPair() + ": " + e.getMessage());
            }
        }
        exposureProducer.flush();
    }

    private void publishExposureToAeron(List<ExposureSnapshot> snapshots) {
        for (ExposureSnapshot snapshot : snapshots) {
            try {
                byte[] jsonBytes = OBJECT_MAPPER.writeValueAsBytes(snapshot);
                exposureBuffer.putBytes(0, jsonBytes);

                long result = exposurePublication.offer(exposureBuffer, 0, jsonBytes.length);
                if (result < 0L) {
                    if (result == Publication.NOT_CONNECTED) {
                        System.err.println("[EXPOSURE] Aeron: no subscriber connected for "
                                + snapshot.getPair());
                    } else if (result == Publication.BACK_PRESSURED) {
                        System.err.println("[EXPOSURE] Aeron: back-pressured for "
                                + snapshot.getPair());
                    }
                }
            } catch (Exception e) {
                System.err.println("[EXPOSURE] Error publishing snapshot via Aeron for "
                        + snapshot.getPair() + ": " + e.getMessage());
            }
        }
    }

    private KafkaProducer<String, String> createExposureProducer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "1",
                ProducerConfig.LINGER_MS_CONFIG, "0"
        ));
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap,
                ConsumerConfig.GROUP_ID_CONFIG, consumerGroup,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"
        ));
    }

    private void shutdown() {
        System.out.println("\nShutting down ExposureEngine...");
        running = false;
        scheduler.shutdown();
        snapshotAndPublishExposure();
        if (exposureProducer != null) {
            exposureProducer.flush();
            exposureProducer.close(Duration.ofSeconds(5));
        }
        if (exposurePublication != null) {
            exposurePublication.close();
        }
        if (sharedAeron != null) sharedAeron.close();
        if (sharedDriver != null) sharedDriver.close();
        System.out.println("ExposureEngine stopped.");
    }

    private static String envOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
