package com.github.programmingwithmati.exposureEngine;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateEvent;
import com.github.programmingwithmati.exposureEngine.model.OrderUpdateKafkaPayload;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Connects to the Binance Futures private WebSocket (user data stream),
 * listens for ORDER_TRADE_UPDATE events, and publishes them to a Kafka topic.
 *
 * Required env vars:
 *   BINANCE_API_KEY       – your Binance API key
 *   BINANCE_API_SECRET    – your Binance API secret (used for HMAC-SHA256 signing)
 *   KAFKA_BOOTSTRAP       – Kafka broker address (default: localhost:9092)
 *   BINANCE_FUTURES_URL   – REST base URL (default: https://fapi.binance.com)
 *   BINANCE_WS_URL        – WebSocket base URL (default: wss://fstream.binance.com)
 */
public class BinanceOrderUpdateProducer {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String KAFKA_TOPIC = "binance_order_updates";
    private static final String EVENT_TYPE_ORDER_UPDATE = "ORDER_TRADE_UPDATE";
    private static final long KEEPALIVE_INTERVAL_MINUTES = 30;

    private final String apiKey;
    private final String apiSecret;
    private final String futuresRestUrl;
    private final String futuresWsUrl;
    private final String kafkaBootstrap;

    private final HttpClient httpClient;
    private KafkaProducer<String, String> kafkaProducer;
    private WebSocket webSocket;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private volatile String listenKey;
    private volatile boolean running = true;

    public BinanceOrderUpdateProducer() {
        this.apiKey = requireEnv("BINANCE_API_KEY");
        this.apiSecret = requireEnv("BINANCE_API_SECRET");
        this.kafkaBootstrap = envOrDefault("KAFKA_BOOTSTRAP", "localhost:9092");
        this.futuresRestUrl = envOrDefault("BINANCE_FUTURES_URL", "https://fapi.binance.com");
        this.futuresWsUrl = envOrDefault("BINANCE_WS_URL", "wss://fstream.binance.com");
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    public static void main(String[] args) {
        BinanceOrderUpdateProducer producer = new BinanceOrderUpdateProducer();
        Runtime.getRuntime().addShutdownHook(new Thread(producer::shutdown));
        producer.start();
    }

    public void start() {
        kafkaProducer = createKafkaProducer();
        listenKey = createListenKey();
        System.out.println("Obtained listen key: " + listenKey.substring(0, 8) + "...");

        scheduleListenKeyKeepalive();
        connectWebSocket();

        System.out.println("BinanceOrderUpdateProducer running. Waiting for ORDER_TRADE_UPDATE events...");
        while (running) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void connectWebSocket() {
        String wsUrl = futuresWsUrl + "/ws/" + listenKey;
        System.out.println("Connecting to WebSocket: " + wsUrl.substring(0, 40) + "...");

        try {
            webSocket = httpClient.newWebSocketBuilder()
                    .buildAsync(URI.create(wsUrl), new BinanceWebSocketListener())
                    .get(10, TimeUnit.SECONDS);
            System.out.println("WebSocket connected.");
        } catch (Exception e) {
            System.err.println("Failed to connect WebSocket: " + e.getMessage());
            scheduleReconnect();
        }
    }

    private void scheduleReconnect() {
        if (!running) return;
        System.out.println("Scheduling WebSocket reconnect in 5 seconds...");
        scheduler.schedule(() -> {
            if (running) {
                try {
                    listenKey = createListenKey();
                    connectWebSocket();
                } catch (Exception e) {
                    System.err.println("Reconnect failed: " + e.getMessage());
                    scheduleReconnect();
                }
            }
        }, 5, TimeUnit.SECONDS);
    }

    private String createListenKey() {
        try {
            String queryString = buildSignedQueryString();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(futuresRestUrl + "/fapi/v1/listenKey?" + queryString))
                    .header("X-MBX-APIKEY", apiKey)
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                throw new RuntimeException("Failed to create listen key. HTTP " + response.statusCode() + ": " + response.body());
            }
            var json = OBJECT_MAPPER.readTree(response.body());
            return json.get("listenKey").asText();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Error creating listen key", e);
        }
    }

    private void keepaliveListenKey() {
        try {
            String queryString = buildSignedQueryString();

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(futuresRestUrl + "/fapi/v1/listenKey?" + queryString))
                    .header("X-MBX-APIKEY", apiKey)
                    .PUT(HttpRequest.BodyPublishers.noBody())
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                System.out.println("Listen key keepalive successful.");
            } else {
                System.err.println("Listen key keepalive failed. HTTP " + response.statusCode() + ": " + response.body());
            }
        } catch (Exception e) {
            System.err.println("Listen key keepalive error: " + e.getMessage());
        }
    }

    private void scheduleListenKeyKeepalive() {
        scheduler.scheduleAtFixedRate(
                this::keepaliveListenKey,
                KEEPALIVE_INTERVAL_MINUTES,
                KEEPALIVE_INTERVAL_MINUTES,
                TimeUnit.MINUTES
        );
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrap,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"
        ));
    }

    private void publishToKafka(OrderUpdateEvent event) {
        try {
            OrderUpdateKafkaPayload payload = OrderUpdateKafkaPayload.fromOrderUpdateEvent(event);
            String key = payload.getPair();
            String value = OBJECT_MAPPER.writeValueAsString(payload);

            kafkaProducer.send(
                    new ProducerRecord<>(KAFKA_TOPIC, key, value),
                    (metadata, exception) -> {
                        if (exception != null) {
                            System.err.println("Kafka send failed for " + key + ": " + exception.getMessage());
                        } else {
                            System.out.println("Published to " + KAFKA_TOPIC +
                                    " [partition=" + metadata.partition() +
                                    ", offset=" + metadata.offset() +
                                    "] key=" + key);
                        }
                    }
            );
        } catch (Exception e) {
            System.err.println("Error publishing to Kafka: " + e.getMessage());
        }
    }

    private class BinanceWebSocketListener implements WebSocket.Listener {
        private final StringBuilder messageBuffer = new StringBuilder();

        @Override
        public void onOpen(WebSocket webSocket) {
            System.out.println("WebSocket opened.");
            WebSocket.Listener.super.onOpen(webSocket);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            messageBuffer.append(data);
            if (last) {
                processMessage(messageBuffer.toString());
                messageBuffer.setLength(0);
            }
            return WebSocket.Listener.super.onText(webSocket, data, last);
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            System.out.println("WebSocket closed: " + statusCode + " " + reason);
            scheduleReconnect();
            return WebSocket.Listener.super.onClose(webSocket, statusCode, reason);
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            System.err.println("WebSocket error: " + error.getMessage());
            scheduleReconnect();
        }
    }

    private void processMessage(String rawJson) {
        try {
            var tree = OBJECT_MAPPER.readTree(rawJson);
            String eventType = tree.has("e") ? tree.get("e").asText() : null;

            if (!EVENT_TYPE_ORDER_UPDATE.equals(eventType)) {
                return;
            }

            OrderUpdateEvent event = OBJECT_MAPPER.treeToValue(tree, OrderUpdateEvent.class);
            OrderUpdateEvent.OrderData o = event.getOrder();

            System.out.printf("ORDER_TRADE_UPDATE | %s %s %s qty=%s price=%s status=%s exec=%s%n",
                    o.getSymbol(), o.getSide(), o.getOrderType(),
                    o.getOriginalQuantity(), o.getOriginalPrice(),
                    o.getOrderStatus(), o.getExecutionType());

            publishToKafka(event);

        } catch (Exception e) {
            System.err.println("Error processing message: " + e.getMessage());
            System.err.println("Raw: " + rawJson.substring(0, Math.min(200, rawJson.length())));
        }
    }

    private String buildSignedQueryString() {
        String queryString = "timestamp=" + System.currentTimeMillis();
        String signature = hmacSha256(apiSecret, queryString);
        return queryString + "&signature=" + signature;
    }

    private static String hmacSha256(String secret, String data) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            SecretKeySpec keySpec = new SecretKeySpec(
                    secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256");
            mac.init(keySpec);
            byte[] rawHmac = mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
            StringBuilder hexString = new StringBuilder(rawHmac.length * 2);
            for (byte b : rawHmac) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception e) {
            throw new RuntimeException("Failed to compute HMAC-SHA256 signature", e);
        }
    }

    public void shutdown() {
        System.out.println("Shutting down BinanceOrderUpdateProducer...");
        running = false;
        scheduler.shutdown();

        if (webSocket != null) {
            webSocket.sendClose(WebSocket.NORMAL_CLOSURE, "shutdown");
        }
        if (kafkaProducer != null) {
            kafkaProducer.flush();
            kafkaProducer.close(Duration.ofSeconds(5));
        }
        System.out.println("Shutdown complete.");
    }

    private static String requireEnv(String key) {
        String val = System.getenv(key);
        if (val == null || val.isBlank()) {
            throw new IllegalStateException("Required environment variable not set: " + key);
        }
        return val;
    }

    private static String envOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
