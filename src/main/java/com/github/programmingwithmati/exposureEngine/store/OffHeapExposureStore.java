package com.github.programmingwithmati.exposureEngine.store;

import com.github.programmingwithmati.exposureEngine.model.ExposureSnapshot;
import org.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Authoritative off-heap exposure state store.
 *
 * Tracks per-pair net quantity: buys add, sells subtract.
 * All numerical state lives in an off-heap {@link UnsafeBuffer} — only the
 * pair-to-slot index is on-heap ({@link ConcurrentHashMap}).
 *
 * <h3>Off-heap slot layout (64 bytes per pair):</h3>
 * <pre>
 *   Offset  Size  Field
 *   ------  ----  -----
 *     0       8   netQuantity       (double)  — positive = long, negative = short
 *     8       8   totalBuyQuantity  (double)
 *    16       8   totalSellQuantity (double)
 *    24       8   totalBuyNotional  (double)  — cumulative (qty × price) for buys
 *    32       8   totalSellNotional (double)  — cumulative (qty × price) for sells
 *    40       4   tradeCount        (int)
 *    44       8   lastUpdateTs      (long)    — epoch millis
 *    52      12   (padding to 64-byte alignment)
 * </pre>
 *
 * Thread-safe: concurrent updates from Kafka and Aeron threads are serialized
 * per-pair via fine-grained locking.
 */
public class OffHeapExposureStore {

    private static final int SLOT_SIZE = 64;
    private static final int MAX_PAIRS = 512;

    private static final int OFF_NET_QTY = 0;
    private static final int OFF_BUY_QTY = 8;
    private static final int OFF_SELL_QTY = 16;
    private static final int OFF_BUY_NOTIONAL = 24;
    private static final int OFF_SELL_NOTIONAL = 32;
    private static final int OFF_TRADE_COUNT = 40;
    private static final int OFF_LAST_TS = 44;

    private final UnsafeBuffer buffer;
    private final ConcurrentHashMap<String, Integer> pairToSlot = new ConcurrentHashMap<>();
    private final Object[] slotLocks;
    private volatile int nextSlot = 0;

    public OffHeapExposureStore() {
        ByteBuffer directBuffer = ByteBuffer.allocateDirect(MAX_PAIRS * SLOT_SIZE);
        this.buffer = new UnsafeBuffer(directBuffer);
        this.slotLocks = new Object[MAX_PAIRS];
        for (int i = 0; i < MAX_PAIRS; i++) {
            slotLocks[i] = new Object();
        }
    }

    /**
     * Apply a fill to the exposure state.
     *
     * @param pair      instrument pair (e.g. "B-BTC_USDT")
     * @param side      "buy" or "sell" (lowercase)
     * @param quantity  filled quantity
     * @param price     fill price
     * @param timestamp epoch millis of the fill
     * @param source    label for logging ("KAFKA" or "AERON")
     */
    public void applyFill(String pair, String side, double quantity, double price, long timestamp, String source) {
        long startNs = System.nanoTime();

        int slot = getOrAllocateSlot(pair);
        int base = slot * SLOT_SIZE;

        synchronized (slotLocks[slot]) {
            double currentNet = buffer.getDouble(base + OFF_NET_QTY);
            double buyQty = buffer.getDouble(base + OFF_BUY_QTY);
            double sellQty = buffer.getDouble(base + OFF_SELL_QTY);
            double buyNotional = buffer.getDouble(base + OFF_BUY_NOTIONAL);
            double sellNotional = buffer.getDouble(base + OFF_SELL_NOTIONAL);
            int tradeCount = buffer.getInt(base + OFF_TRADE_COUNT);

            if ("buy".equalsIgnoreCase(side)) {
                currentNet += quantity;
                buyQty += quantity;
                buyNotional += quantity * price;
            } else {
                currentNet -= quantity;
                sellQty += quantity;
                sellNotional += quantity * price;
            }

            buffer.putDouble(base + OFF_NET_QTY, currentNet);
            buffer.putDouble(base + OFF_BUY_QTY, buyQty);
            buffer.putDouble(base + OFF_SELL_QTY, sellQty);
            buffer.putDouble(base + OFF_BUY_NOTIONAL, buyNotional);
            buffer.putDouble(base + OFF_SELL_NOTIONAL, sellNotional);
            buffer.putInt(base + OFF_TRADE_COUNT, tradeCount + 1);
            buffer.putLong(base + OFF_LAST_TS, timestamp);

            long elapsedNs = System.nanoTime() - startNs;
            double elapsedUs = elapsedNs / 1_000.0;

            System.out.printf("[%s] EXPOSURE UPDATE | pair=%-15s side=%-4s qty=%.6f price=%.4f => " +
                            "netQty=%.6f buyQty=%.6f sellQty=%.6f trades=%d | %.2f µs%n",
                    source, pair, side, quantity, price,
                    currentNet, buyQty, sellQty, tradeCount + 1, elapsedUs);
        }
    }

    public double getNetQuantity(String pair) {
        Integer slot = pairToSlot.get(pair);
        if (slot == null) return 0.0;
        return buffer.getDouble(slot * SLOT_SIZE + OFF_NET_QTY);
    }

    public double getTotalBuyQuantity(String pair) {
        Integer slot = pairToSlot.get(pair);
        if (slot == null) return 0.0;
        return buffer.getDouble(slot * SLOT_SIZE + OFF_BUY_QTY);
    }

    public double getTotalSellQuantity(String pair) {
        Integer slot = pairToSlot.get(pair);
        if (slot == null) return 0.0;
        return buffer.getDouble(slot * SLOT_SIZE + OFF_SELL_QTY);
    }

    public double getTotalBuyNotional(String pair) {
        Integer slot = pairToSlot.get(pair);
        if (slot == null) return 0.0;
        return buffer.getDouble(slot * SLOT_SIZE + OFF_BUY_NOTIONAL);
    }

    public double getTotalSellNotional(String pair) {
        Integer slot = pairToSlot.get(pair);
        if (slot == null) return 0.0;
        return buffer.getDouble(slot * SLOT_SIZE + OFF_SELL_NOTIONAL);
    }

    public int getTradeCount(String pair) {
        Integer slot = pairToSlot.get(pair);
        if (slot == null) return 0;
        return buffer.getInt(slot * SLOT_SIZE + OFF_TRADE_COUNT);
    }

    /**
     * Returns a consistent snapshot of the exposure state for all tracked pairs.
     */
    public List<ExposureSnapshot> getExposureSnapshots() {
        long snapshotTs = System.currentTimeMillis();
        List<ExposureSnapshot> snapshots = new ArrayList<>();

        for (Map.Entry<String, Integer> entry : pairToSlot.entrySet()) {
            String pair = entry.getKey();
            int base = entry.getValue() * SLOT_SIZE;

            synchronized (slotLocks[entry.getValue()]) {
                snapshots.add(ExposureSnapshot.builder()
                        .pair(pair)
                        .netQuantity(buffer.getDouble(base + OFF_NET_QTY))
                        .totalBuyQuantity(buffer.getDouble(base + OFF_BUY_QTY))
                        .totalSellQuantity(buffer.getDouble(base + OFF_SELL_QTY))
                        .totalBuyNotional(buffer.getDouble(base + OFF_BUY_NOTIONAL))
                        .totalSellNotional(buffer.getDouble(base + OFF_SELL_NOTIONAL))
                        .tradeCount(buffer.getInt(base + OFF_TRADE_COUNT))
                        .lastUpdateTs(buffer.getLong(base + OFF_LAST_TS))
                        .snapshotTs(snapshotTs)
                        .build());
            }
        }

        return snapshots;
    }

    /**
     * Prints the full exposure state table to console for all tracked pairs.
     */
    public void printExposureSnapshot() {
        List<ExposureSnapshot> snapshots = getExposureSnapshots();

        System.out.println("\n╔══════════════════════════════════════════════════════════════════════════════════════╗");
        System.out.println("║                        AUTHORITATIVE OFF-HEAP EXPOSURE STATE                       ║");
        System.out.println("╠═════════════════╦════════════════╦════════════════╦════════════════╦════════════════╣");
        System.out.printf("║ %-15s ║ %14s ║ %14s ║ %14s ║ %14s ║%n",
                "Pair", "Net Qty", "Buy Qty", "Sell Qty", "Trades");
        System.out.println("╠═════════════════╬════════════════╬════════════════╬════════════════╬════════════════╣");

        for (ExposureSnapshot snap : snapshots) {
            System.out.printf("║ %-15s ║ %+14.6f ║ %14.6f ║ %14.6f ║ %14d ║%n",
                    snap.getPair(), snap.getNetQuantity(),
                    snap.getTotalBuyQuantity(), snap.getTotalSellQuantity(),
                    snap.getTradeCount());
        }

        System.out.println("╚═════════════════╩════════════════╩════════════════╩════════════════╩════════════════╝\n");
    }

    private int getOrAllocateSlot(String pair) {
        return pairToSlot.computeIfAbsent(pair, k -> {
            int slot = nextSlot++;
            if (slot >= MAX_PAIRS) {
                throw new IllegalStateException("Max pairs (" + MAX_PAIRS + ") exceeded");
            }
            return slot;
        });
    }
}
