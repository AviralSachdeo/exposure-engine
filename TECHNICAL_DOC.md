# Exposure Engine — Technical Documentation

## 1. Overview

The Exposure Engine is a real-time risk aggregation service written in Java. It joins two independent trade/fill streams — Binance Futures order updates (external exchange) and internal matching-engine fills — into a single authoritative off-heap exposure state. Each stream's transport layer (Apache Kafka or Aeron) is independently configurable at runtime, enabling flexible deployment topologies.

### Core Responsibilities

- **Stream Ingestion** — Consume trade events from Binance and the internal matching engine over Kafka or Aeron.
- **Stream Joining** — Correlate both streams into a unified exposure view per instrument pair.
- **Off-Heap State Management** — Maintain a GC-free, low-latency exposure store outside the JVM heap using Agrona's `UnsafeBuffer`.
- **Exposure Calculation** — For each fill: add quantity on buy, subtract quantity on sell, and accumulate notional values.
- **Exposure Publishing** — Snapshot the off-heap state every 5 seconds and publish per-pair `ExposureSnapshot` records to a Kafka topic or Aeron channel.

### Key Design Goals

| Goal | Approach |
|---|---|
| Low latency | Off-heap state (no GC pauses), Aeron transport option |
| Flexibility | Transport layer per stream is runtime-configurable (including exposure output) |
| Thread safety | Fine-grained per-pair locking on off-heap slots |
| Simplicity | Single-process, multi-threaded architecture |

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         EXPOSURE ENGINE                             │
│                                                                     │
│  ┌──────────────┐    ┌──────────────┐    ┌────────────────────────┐ │
│  │   Binance     │    │  Matching    │    │   Off-Heap Exposure    │ │
│  │   Stream      │    │  Engine      │    │   Store                │ │
│  │   Consumer    │    │  Stream      │    │   (UnsafeBuffer)       │ │
│  │              │    │  Consumer    │    │                        │ │
│  │  Kafka  OR   │    │  Kafka  OR   │    │  ┌──────────────────┐  │ │
│  │  Aeron       │    │  Aeron       │    │  │ B-BTC_USDT  [64B]│  │ │
│  └──────┬───────┘    └──────┬───────┘    │  │ B-ETH_USDT  [64B]│  │ │
│         │                   │            │  │ B-DOGE_USDT [64B]│  │ │
│         │   applyFill()     │            │  │ ...              │  │ │
│         └─────────┬─────────┘            │  └──────────────────┘  │ │
│                   │                      └────────────────────────┘ │
│                   ▼                                                 │
│          ┌────────────────┐                                         │
│          │  applyFill()   │  buy → netQty += qty                    │
│          │                │  sell → netQty -= qty                    │
│          └────────────────┘                                         │
│                                                                     │
│          ┌────────────────────────────┐                              │
│          │  snapshotAndPublish        │  Every 5 seconds:            │
│          │  Exposure()                │  1. Print state to console   │
│          │  (ScheduledExecutorService)│  2. Publish ExposureSnapshot │
│          │                            │     per pair → Kafka or Aeron│
│          └────────────────────────────┘                              │
└─────────────────────────────────────────────────────────────────────┘

External Data Sources:
  ┌──────────────────┐          ┌──────────────────────┐
  │  Binance Futures  │          │  Internal Matching    │
  │  WebSocket API    │          │  Engine               │
  │  (ORDER_TRADE_    │          │  (Aeron/Kafka fills)  │
  │   UPDATE events)  │          │                       │
  └──────────────────┘          └──────────────────────┘
```

### Thread Model

| Thread | Responsibility |
|---|---|
| `binance-kafka-consumer` OR `binance-aeron-subscriber` | Consumes Binance order updates |
| `me-kafka-consumer` OR `me-aeron-subscriber` | Consumes matching-engine fills |
| `main` | Lifecycle management, keeps process alive |
| `ScheduledExecutorService` | Runs `snapshotAndPublishExposure()` every 5s — prints and publishes to Kafka or Aeron |

---

## 3. Project Structure

```
exposure-engine-java/
├── pom.xml                                  # Maven build (Java 17, Aeron, Kafka, Jackson, Lombok)
├── mvnw / mvnw.cmd                          # Maven wrapper (no Maven install required)
├── .gitignore
│
├── loadTestExposureEngine/                  # Benchmark scripts & reports (outside src/)
│   ├── LOAD_TEST_REPORT.md                  # Latency benchmark results
│   ├── run-benchmark.sh                     # Runner: processing benchmark (no Kafka needed)
│   └── run-transport-benchmark.sh           # Runner: transport benchmark (Kafka needed)
│
└── src/main/java/com/github/programmingwithmati/exposureEngine/
    ├── ExposureEngine.java                  # Main orchestrator — joins streams, manages lifecycle
    ├── BinanceOrderUpdateProducer.java      # WebSocket → Kafka bridge for Binance
    ├── OrderUpdateConsumer.java             # Standalone Kafka consumer for Binance (debugging)
    ├── MatchingEngineAeronSubscriber.java   # Standalone Aeron subscriber for ME (debugging)
    │
    ├── model/
    │   ├── OrderUpdateEvent.java            # Raw Binance WebSocket event model
    │   ├── OrderUpdateKafkaPayload.java     # Normalized Kafka message format for Binance
    │   ├── MatchingEngineFill.java          # Matching engine fill event model
    │   └── ExposureSnapshot.java            # Per-pair exposure snapshot published to Kafka/Aeron
    │
    ├── store/
    │   └── OffHeapExposureStore.java        # Authoritative off-heap exposure state
    │
    ├── testPublishers/                      # Small-scale test data publishers (5 events each)
    │   ├── kafka/
    │   │   ├── BinanceTestKafkaProducer.java
    │   │   └── METestKafkaProducer.java
    │   └── aeron/
    │       ├── BinanceTestAeronPublisher.java
    │       └── MatchingEngineTestPublisher.java
    │
    ├── loadTestPublishers/                  # High-volume load test publishers (10k+ msgs)
    │   ├── kafka/
    │   │   └── LoadTestKafkaPublisher.java          # Both streams via Kafka
    │   ├── aeron/
    │   │   └── LoadTestAeronPublisher.java          # Both streams via Aeron
    │   ├── kafka_aeron/
    │   │   └── LoadTestKafkaAeronPublisher.java     # Binance=Kafka, ME=Aeron
    │   └── aeron_kafka/
    │       └── LoadTestAeronKafkaPublisher.java     # Binance=Aeron, ME=Kafka
    │
    └── loadTestExposureEngine/              # Latency benchmarks (in-process)
        ├── ExposureEngineLatencyBenchmark.java  # Processing pipeline latency (no Kafka/Aeron)
        └── TransportLatencyBenchmark.java       # End-to-end transport latency (all 4 scenarios)
```

---

## 4. Components in Detail

### 4.1 ExposureEngine (Main Orchestrator)

**Class:** `com.github.programmingwithmati.exposureEngine.ExposureEngine`

The central component that:

1. Reads transport configuration from environment variables.
2. Initializes exposure output — creates a Kafka producer **or** an Aeron publication for publishing exposure snapshots.
3. Starts a shared Aeron `MediaDriver` if **any** stream (Binance, ME, or exposure output) uses Aeron.
4. Spawns daemon threads for each stream consumer (Kafka or Aeron).
5. Routes incoming events through `applyFill()` into the `OffHeapExposureStore`.
6. Schedules `snapshotAndPublishExposure()` every 5 seconds, which:
   - Prints the exposure state table to console via `store.printExposureSnapshot()`.
   - Retrieves `ExposureSnapshot` objects via `store.getExposureSnapshots()`.
   - Publishes each snapshot as JSON to the configured output (Kafka topic **or** Aeron channel).

**Stream Processing Logic:**

- **Binance events** — Only `FILLED` and `PARTIALLY_FILLED` order statuses are processed; all others are discarded.
- **Matching engine events** — All fills are processed unconditionally.
- Both streams call the same `OffHeapExposureStore.applyFill()` method, ensuring a unified view.

**Exposure Output Transport:**

The `EXPOSURE_TRANSPORT` environment variable controls whether snapshots are published via Kafka or Aeron:

| `EXPOSURE_TRANSPORT` | Behavior |
|---|---|
| `kafka` (default) | Publishes JSON to the `exposure_snapshots` Kafka topic, keyed by pair |
| `aeron` | Publishes JSON bytes to an Aeron publication on the configured channel/stream |

### 4.2 OffHeapExposureStore

**Class:** `com.github.programmingwithmati.exposureEngine.store.OffHeapExposureStore`

The authoritative source of truth for exposure state. All numerical data lives outside the JVM heap in a `DirectByteBuffer` accessed via Agrona's `UnsafeBuffer`.

**Why off-heap?**

- **No GC pauses** — Critical for low-latency financial systems.
- **Predictable memory layout** — Fixed 64-byte slots enable cache-friendly access.
- **Shared memory potential** — `DirectByteBuffer` can be memory-mapped for cross-process sharing (future enhancement).

**Slot Layout (64 bytes per instrument pair):**

```
Offset  Size  Type    Field
──────  ────  ──────  ─────────────────
  0       8   double  netQuantity        (positive = long, negative = short)
  8       8   double  totalBuyQuantity
 16       8   double  totalSellQuantity
 24       8   double  totalBuyNotional   (cumulative qty × price for buys)
 32       8   double  totalSellNotional  (cumulative qty × price for sells)
 40       4   int     tradeCount
 44       8   long    lastUpdateTs       (epoch milliseconds)
 52      12   —       padding (64-byte alignment)
```

**Capacity:** 512 pairs × 64 bytes = 32 KB total off-heap allocation.

**Thread Safety:** A per-slot synchronized lock array (`Object[512]`) ensures that concurrent updates from Kafka and Aeron threads on the same pair are serialized. Different pairs can be updated concurrently without contention.

**Key Methods:**

| Method | Description |
|---|---|
| `applyFill(pair, side, qty, price, ts, source)` | Updates the off-heap state for a single fill |
| `getExposureSnapshots()` | Returns a `List<ExposureSnapshot>` with a consistent read of all tracked pairs |
| `printExposureSnapshot()` | Prints the exposure state table to console (internally calls `getExposureSnapshots()`) |

**Exposure Calculation:**

```
if side == "buy":
    netQuantity     += quantity
    totalBuyQuantity += quantity
    totalBuyNotional += quantity × price
if side == "sell":
    netQuantity      -= quantity
    totalSellQuantity += quantity
    totalSellNotional += quantity × price
tradeCount += 1
lastUpdateTs = fill timestamp
```

### 4.3 BinanceOrderUpdateProducer

**Class:** `com.github.programmingwithmati.exposureEngine.BinanceOrderUpdateProducer`

A WebSocket-to-Kafka bridge that:

1. Obtains a listen key from the Binance Futures REST API using HMAC-SHA256 signed requests.
2. Connects to the Binance Futures User Data Stream WebSocket.
3. Filters for `ORDER_TRADE_UPDATE` events.
4. Converts raw Binance events (`OrderUpdateEvent`) into the normalized `OrderUpdateKafkaPayload` format.
5. Publishes to the `binance_order_updates` Kafka topic.
6. Schedules listen key keepalive every 30 minutes.
7. Auto-reconnects on WebSocket close/error.

**Authentication Flow:**

```
1. Build query string:  timestamp=<epoch_millis>
2. Sign with HMAC-SHA256: signature = HMAC-SHA256(apiSecret, queryString)
3. POST /fapi/v1/listenKey?timestamp=...&signature=...
   Header: X-MBX-APIKEY: <apiKey>
4. Response: { "listenKey": "..." }
5. Connect WebSocket: wss://fstream.binancefuture.com/ws/<listenKey>
```

> **Note:** `BINANCE_API_KEY` and `BINANCE_API_SECRET` are **required** environment variables. The producer will fail at startup if either is missing.

### 4.4 Data Models

#### OrderUpdateEvent (Raw Binance)

Maps the full Binance `ORDER_TRADE_UPDATE` WebSocket event using Jackson `@JsonProperty` annotations with Binance's single-letter field codes:

| JSON Field | Java Field | Description |
|---|---|---|
| `e` | `eventType` | Event type (`ORDER_TRADE_UPDATE`) |
| `E` | `eventTime` | Event timestamp |
| `T` | `transactionTime` | Transaction timestamp |
| `o` | `order` | Nested `OrderData` object |
| `o.s` | `symbol` | Trading pair (e.g., `BTCUSDT`) |
| `o.S` | `side` | `BUY` or `SELL` |
| `o.l` | `lastFilledQuantity` | Last fill quantity |
| `o.L` | `lastFilledPrice` | Last fill price |
| `o.X` | `orderStatus` | `NEW`, `PARTIALLY_FILLED`, `FILLED`, `CANCELED`, etc. |
| `o.t` | `tradeId` | Exchange trade ID |
| `o.m` | `makerSide` | Whether the fill was on the maker side |

#### OrderUpdateKafkaPayload (Normalized)

The standardized format published to Kafka and used internally:

```json
{
  "derivatives_futures_order_id": "client-order-id",
  "quantity": "1.5",
  "price": "65100.00",
  "pair": "B-BTC_USDT",
  "exchange_trade_id": 12345,
  "ecode": "B",
  "order_status": "FILLED",
  "timestamp": 1710000000000,
  "order_filled_quantity": "1.5",
  "order_avg_price": "65100.00",
  "side": "buy",
  "is_maker": false
}
```

**Symbol Normalization:** Binance symbols (e.g., `BTCUSDT`) are converted to the internal format `B-BTC_USDT` by stripping known quote currencies (`USDT`, `BUSD`, `USDC`, `BTC`, `ETH`).

#### MatchingEngineFill

```json
{
  "derivatives_futures_order_id": "ME-ORD-001",
  "quantity": "0.5",
  "price": "65200.50",
  "pair": "B-BTC_USDT",
  "exchange_trade_id": "ME-TRD-1001",
  "side": "buy",
  "timestamp": 1710000000000,
  "is_maker": false
}
```

#### ExposureSnapshot

Published to the `exposure_snapshots` Kafka topic (or Aeron channel) every 5 seconds for each tracked pair:

```json
{
  "pair": "B-BTC_USDT",
  "net_quantity": 0.4,
  "total_buy_quantity": 1.5,
  "total_sell_quantity": 1.1,
  "total_buy_notional": 97650.0,
  "total_sell_notional": 71775.0,
  "trade_count": 4,
  "last_update_ts": 1710000000000,
  "snapshot_ts": 1710000005000
}
```

| Field | Type | Description |
|---|---|---|
| `pair` | string | Instrument pair identifier |
| `net_quantity` | double | Net exposure (positive = long, negative = short) |
| `total_buy_quantity` | double | Cumulative buy quantity |
| `total_sell_quantity` | double | Cumulative sell quantity |
| `total_buy_notional` | double | Cumulative buy value (qty × price) |
| `total_sell_notional` | double | Cumulative sell value (qty × price) |
| `trade_count` | int | Total number of fills applied |
| `last_update_ts` | long | Epoch millis of the last fill that modified this pair |
| `snapshot_ts` | long | Epoch millis when the snapshot was taken |

### 4.5 Standalone Components

| Class | Purpose |
|---|---|
| `OrderUpdateConsumer` | Standalone Kafka consumer for Binance order updates. Useful for debugging/monitoring the Kafka topic independently. |
| `MatchingEngineAeronSubscriber` | Standalone Aeron subscriber for matching-engine fills. Useful for debugging/monitoring the Aeron stream independently. |

---

## 5. Transport Configuration

Each stream's transport is configured independently via environment variables:

| Env Variable | Values | Default | Description |
|---|---|---|---|
| `BINANCE_TRANSPORT` | `kafka` / `aeron` | `kafka` | Transport for Binance order updates |
| `ME_TRANSPORT` | `kafka` / `aeron` | `kafka` | Transport for matching engine fills |
| `EXPOSURE_TRANSPORT` | `kafka` / `aeron` | `kafka` | Transport for exposure snapshot output |

### 4 Supported Input Scenarios

| Scenario | `BINANCE_TRANSPORT` | `ME_TRANSPORT` | Use Case |
|---|---|---|---|
| 1 | `kafka` | `kafka` | Standard deployment, both streams via message broker |
| 2 | `kafka` | `aeron` | External via broker, internal via ultra-low-latency IPC |
| 3 | `aeron` | `aeron` | Maximum performance, both via shared memory |
| 4 | `aeron` | `kafka` | Internal via Aeron, external via broker |

### Full Environment Variable Reference

**Transport Selection:**

| Variable | Default | Description |
|---|---|---|
| `BINANCE_TRANSPORT` | `kafka` | `kafka` or `aeron` |
| `ME_TRANSPORT` | `kafka` | `kafka` or `aeron` |
| `EXPOSURE_TRANSPORT` | `kafka` | `kafka` or `aeron` |

**Kafka Configuration:**

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |
| `CONSUMER_GROUP` | `exposure-engine` | Consumer group ID |
| `BINANCE_KAFKA_TOPIC` | `binance_order_updates` | Kafka topic for Binance events |
| `ME_KAFKA_TOPIC` | `me_fills` | Kafka topic for ME fills |
| `EXPOSURE_KAFKA_TOPIC` | `exposure_snapshots` | Kafka topic for exposure snapshot output |

**Aeron Configuration:**

| Variable | Default | Description |
|---|---|---|
| `BINANCE_AERON_CHANNEL` | `aeron:udp?endpoint=localhost:40457` | Aeron channel for Binance |
| `BINANCE_AERON_STREAM_ID` | `30` | Aeron stream ID for Binance |
| `ME_AERON_CHANNEL` | `aeron:udp?endpoint=localhost:40456` | Aeron channel for ME fills |
| `ME_AERON_STREAM_ID` | `20` | Aeron stream ID for ME fills |
| `EXPOSURE_AERON_CHANNEL` | `aeron:udp?endpoint=localhost:40458` | Aeron channel for exposure output |
| `EXPOSURE_AERON_STREAM_ID` | `40` | Aeron stream ID for exposure output |

**Binance API (for `BinanceOrderUpdateProducer`):**

| Variable | Default | Description |
|---|---|---|
| `BINANCE_API_KEY` | *(required)* | Binance API key |
| `BINANCE_API_SECRET` | *(required)* | Binance API secret (HMAC-SHA256 signing) |
| `BINANCE_FUTURES_URL` | `https://fapi.binance.com` | REST API base URL |
| `BINANCE_WS_URL` | `wss://fstream.binance.com` | WebSocket base URL |

---

## 6. Technology Stack

| Technology | Version | Purpose |
|---|---|---|
| Java | 17 (compile target; tested on JDK 24) | Runtime |
| Apache Kafka | 3.7.0 (`kafka-clients`) | Distributed message streaming |
| Aeron | 1.46.7 | Ultra-low-latency message transport |
| Agrona | 1.23.1 | Off-heap memory primitives (`UnsafeBuffer`) |
| Jackson | 2.17.0 (`jackson-databind`) | JSON serialization/deserialization |
| Lombok | 1.18.44 | Boilerplate reduction (`@Data`, `@Builder`) |
| SLF4J + Simple | 2.0.12 | Logging |
| JUnit Jupiter | 5.10.2 | Testing |
| Maven | 3.9.6 (wrapper included) | Build & dependency management |

### JVM Requirements

Aeron/Agrona require access to internal JDK APIs. The following JVM flags are mandatory:

```
--add-opens java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
```

---

## 7. Build & Run

### Prerequisites

- **Java 17+** (tested on JDK 24)
- **Apache Kafka** running on `localhost:9092` (for Kafka transport scenarios)
- **Maven** (or use the included `./mvnw` wrapper — no Maven install required)

### Compile

```bash
export JAVA_HOME="<path-to-jdk>"
export PATH="$JAVA_HOME/bin:$PATH"
./mvnw clean compile
```

### Build Classpath

```bash
./mvnw -q dependency:copy-dependencies -DoutputDirectory=target/dependency
export CP="target/classes:$(echo target/dependency/*.jar | tr ' ' ':')"
export AERON_FLAGS="--add-opens java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED"
```

### Run ExposureEngine

```bash
BINANCE_TRANSPORT=kafka ME_TRANSPORT=aeron \
java $AERON_FLAGS -cp "$CP" \
com.github.programmingwithmati.exposureEngine.ExposureEngine
```

### Run BinanceOrderUpdateProducer (Live Binance Data)

```bash
BINANCE_API_KEY=<your-key> BINANCE_API_SECRET=<your-secret> \
java -cp "$CP" \
com.github.programmingwithmati.exposureEngine.BinanceOrderUpdateProducer
```

---

## 8. Testing

### 8.1 Test Publishers (Small-Scale, 5 Events Each)

Four test publishers generate synthetic data across 4 instrument pairs:

| Publisher | Transport | Topic/Channel | Command |
|---|---|---|---|
| `BinanceTestKafkaProducer` | Kafka | `binance_order_updates` | `java -cp "$CP" ...testPublishers.kafka.BinanceTestKafkaProducer` |
| `METestKafkaProducer` | Kafka | `me_fills` | `java -cp "$CP" ...testPublishers.kafka.METestKafkaProducer` |
| `BinanceTestAeronPublisher` | Aeron | `localhost:40457` stream 30 | `java $AERON_FLAGS -cp "$CP" ...testPublishers.aeron.BinanceTestAeronPublisher` |
| `MatchingEngineTestPublisher` | Aeron | `localhost:40456` stream 20 | `java $AERON_FLAGS -cp "$CP" ...testPublishers.aeron.MatchingEngineTestPublisher` |

### 8.2 Load Test Publishers (High-Volume, 10,000+ Messages)

Four load test publishers for stress-testing all transport combinations. Each sends 10,000 messages (configurable via `LOAD_TEST_TOTAL`) split evenly between Binance and ME streams for a single pair (`B-BTC_USDT` by default, configurable via `LOAD_TEST_PAIR`).

| Publisher | Binance Transport | ME Transport | Command |
|---|---|---|---|
| `LoadTestKafkaPublisher` | Kafka | Kafka | `java $AERON_FLAGS -cp "$CP" ...loadTestPublishers.kafka.LoadTestKafkaPublisher` |
| `LoadTestAeronPublisher` | Aeron | Aeron | `java $AERON_FLAGS -cp "$CP" ...loadTestPublishers.aeron.LoadTestAeronPublisher` |
| `LoadTestKafkaAeronPublisher` | Kafka | Aeron | `java $AERON_FLAGS -cp "$CP" ...loadTestPublishers.kafka_aeron.LoadTestKafkaAeronPublisher` |
| `LoadTestAeronKafkaPublisher` | Aeron | Kafka | `java $AERON_FLAGS -cp "$CP" ...loadTestPublishers.aeron_kafka.LoadTestAeronKafkaPublisher` |

**Load test environment variables:**

| Variable | Default | Description |
|---|---|---|
| `LOAD_TEST_PAIR` | `B-BTC_USDT` | Instrument pair to use |
| `LOAD_TEST_TOTAL` | `10000` | Total message count |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |

### 8.3 Latency Benchmarks

Two dedicated benchmark classes measure latency at controlled throughput levels (25k, 50k, 100k msgs/sec):

| Benchmark | Class | What It Measures | Dependencies |
|---|---|---|---|
| **Processing** | `ExposureEngineLatencyBenchmark` | Pure processing latency — store-only and full pipeline (JSON deser + parse + applyFill) | None |
| **Transport** | `TransportLatencyBenchmark` | Full end-to-end latency including transport layer for all 4 scenarios | Kafka broker (for Kafka scenarios; Aeron-only runs without Kafka) |

**Runner scripts:**

```bash
# Processing benchmark (no Kafka required)
./loadTestExposureEngine/run-benchmark.sh

# Transport benchmark (Kafka required for 3 of 4 scenarios)
./loadTestExposureEngine/run-transport-benchmark.sh
```

**Benchmark environment variables:**

| Variable | Default | Description |
|---|---|---|
| `LOAD_TEST_DURATION` | `10` | Seconds per throughput level |
| `LOAD_TEST_WARMUP` | `100000` | Warmup message count (processing benchmark only) |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |

See `loadTestExposureEngine/LOAD_TEST_REPORT.md` for detailed benchmark results and analysis.

### 8.4 Test Data (Binance Kafka Producer)

| Order ID | Pair | Side | Qty | Price | Status |
|---|---|---|---|---|---|
| BN-ORD-001 | B-BTC_USDT | buy | 1.2 | 65100.00 | FILLED |
| BN-ORD-002 | B-ETH_USDT | sell | 5.0 | 3420.50 | FILLED |
| BN-ORD-003 | B-BTC_USDT | sell | 0.8 | 65250.00 | PARTIALLY_FILLED |
| BN-ORD-004 | B-DOGE_USDT | buy | 200.0 | 0.1580 | FILLED |
| BN-ORD-005 | B-LTC_USDT | buy | 50.0 | 142.90 | FILLED |

### 8.5 Test Data (ME Kafka Producer)

| Order ID | Pair | Side | Qty | Price |
|---|---|---|---|---|
| ME-ORD-101 | B-BTC_USDT | buy | 0.3 | 65150.00 |
| ME-ORD-102 | B-ETH_USDT | buy | 8.0 | 3415.75 |
| ME-ORD-103 | B-BTC_USDT | sell | 1.5 | 65300.00 |
| ME-ORD-104 | B-DOGE_USDT | buy | 3000.0 | 0.1610 |
| ME-ORD-105 | B-LTC_USDT | sell | 75.0 | 143.20 |

### 8.6 Expected Exposure (Scenario 1 — Both Kafka, Fresh Start)

After sending both Binance and ME test data:

| Pair | Net Qty | Buy Qty | Sell Qty | Trades |
|---|---|---|---|---|
| B-BTC_USDT | +0.200 | 1.500 | 1.300 | 4 |
| B-ETH_USDT | +3.000 | 8.000 | 5.000 | 2 |
| B-DOGE_USDT | +3200.000 | 3200.000 | 0.000 | 2 |
| B-LTC_USDT | -25.000 | 50.000 | 75.000 | 2 |

### 8.7 Running a Full Test Scenario

```bash
# Terminal 1: Start ExposureEngine
BINANCE_TRANSPORT=kafka ME_TRANSPORT=kafka \
java $AERON_FLAGS -cp "$CP" \
com.github.programmingwithmati.exposureEngine.ExposureEngine

# Terminal 2: Send Binance test data
java -cp "$CP" \
com.github.programmingwithmati.exposureEngine.testPublishers.kafka.BinanceTestKafkaProducer

# Terminal 3: Send ME test data
java -cp "$CP" \
com.github.programmingwithmati.exposureEngine.testPublishers.kafka.METestKafkaProducer
```

### 8.8 Cleaning Up Between Test Runs

**Kafka State:**

The consumer uses `auto.offset.reset=earliest`, so previous test data accumulates across runs. To reset:

```bash
kafka-topics --bootstrap-server localhost:9092 --delete --topic binance_order_updates
kafka-topics --bootstrap-server localhost:9092 --delete --topic me_fills
kafka-topics --bootstrap-server localhost:9092 --delete --topic exposure_snapshots
```

**Aeron Ports:**

```bash
lsof -i :40456 -i :40457 -i :40458
kill -9 <PID>
```

---

## 9. Data Flow Diagrams

### Flow A: Binance → Kafka → ExposureEngine

```
Binance Futures       BinanceOrderUpdate     Kafka Topic              ExposureEngine
  WebSocket      →      Producer          →  binance_order_updates  →  Kafka Consumer
                                                                       → applyFill()
  ORDER_TRADE_       OrderUpdateEvent         OrderUpdateKafka           OffHeapExposure
  UPDATE event       → Normalize →            Payload (JSON)             Store
                       Sign HMAC
                       Filter status
```

### Flow B: Matching Engine → Aeron → ExposureEngine

```
Matching             Aeron Publisher          Aeron Channel            ExposureEngine
  Engine         →   (UDP/IPC)           →   localhost:40456       →  Aeron Subscriber
                                              stream=20                → applyFill()
  MatchingEngine      JSON bytes              FragmentHandler           OffHeapExposure
  Fill event          via UnsafeBuffer        → deserialize             Store
```

### Flow C: Joining in the Exposure Store

```
  Binance Fill ──┐
                 ├──→ OffHeapExposureStore.applyFill(pair, side, qty, price, ts, source)
  ME Fill ───────┘
                       │
                       ▼
                 ┌─────────────────────────────────────┐
                 │  Slot for pair (64 bytes off-heap)   │
                 │                                      │
                 │  netQuantity     += qty (buy)        │
                 │  netQuantity     -= qty (sell)       │
                 │  totalBuyQty     += qty (buy)        │
                 │  totalSellQty    += qty (sell)       │
                 │  totalBuyNotional += qty*price (buy) │
                 │  totalSellNotional+= qty*price(sell) │
                 │  tradeCount      += 1                │
                 │  lastUpdateTs    = timestamp          │
                 └─────────────────────────────────────┘
```

### Flow D: Exposure Snapshot Publishing

```
  ScheduledExecutorService (every 5 seconds)
         │
         ▼
  snapshotAndPublishExposure()
         │
         ├──→ store.printExposureSnapshot()     → Console output (state table)
         │
         └──→ store.getExposureSnapshots()      → List<ExposureSnapshot>
                     │
                     ├─── if EXPOSURE_TRANSPORT=kafka ───→ Kafka topic: exposure_snapshots
                     │                                      Key: pair, Value: JSON
                     │
                     └─── if EXPOSURE_TRANSPORT=aeron ───→ Aeron publication
                                                           Channel + stream ID
```

---

## 10. Glossary

| Term | Definition |
|---|---|
| Exposure | The net position (quantity) held in an instrument. Positive = long, negative = short. |
| Off-heap | Memory allocated outside the JVM heap, not subject to garbage collection. |
| Authoritative state | The single source of truth for a piece of data in the system. |
| Aeron | Ultra-low-latency, high-throughput message transport library using shared memory and memory-mapped files. |
| MediaDriver | Aeron's I/O engine that manages network and shared-memory transport. |
| UnsafeBuffer | Agrona's direct memory access wrapper for reading/writing off-heap data. |
| Listen key | A token issued by Binance REST API to authenticate WebSocket connections to user data streams. |
| HMAC-SHA256 | Keyed-hash message authentication code used to sign Binance API requests. |
| ExposureSnapshot | A point-in-time capture of per-pair exposure data, published every 5 seconds. |
| Fill | A trade execution event — an order (or part of an order) has been matched and executed. |
| Notional | The total value of a position: quantity × price. |
| Instrument pair | A trading pair identifier, e.g., `B-BTC_USDT` (Binance BTC/USDT futures). |
| ecode | Exchange code prefix (`B` for Binance). |
| IPC | Inter-Process Communication — Aeron's shared-memory transport mode (zero network overhead). |
