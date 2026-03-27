package com.github.programmingwithmati.exposureEngine.store;

import com.github.programmingwithmati.exposureEngine.model.ExposureSnapshot;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class OffHeapExposureStoreTest {

    private OffHeapExposureStore store;

    @BeforeEach
    void setUp() {
        store = new OffHeapExposureStore();
    }

    @Test
    void unknownPairReturnsZeroDefaults() {
        assertEquals(0.0, store.getNetQuantity("UNKNOWN"));
        assertEquals(0.0, store.getTotalBuyQuantity("UNKNOWN"));
        assertEquals(0.0, store.getTotalSellQuantity("UNKNOWN"));
        assertEquals(0.0, store.getTotalBuyNotional("UNKNOWN"));
        assertEquals(0.0, store.getTotalSellNotional("UNKNOWN"));
        assertEquals(0, store.getTradeCount("UNKNOWN"));
    }

    @Test
    void singleBuyFillUpdatesAllFields() {
        store.applyFill("B-BTC_USDT", "buy", 1.5, 65000.0, 1000L, "TEST");

        assertEquals(1.5, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(1.5, store.getTotalBuyQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(0.0, store.getTotalSellQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(1.5 * 65000.0, store.getTotalBuyNotional("B-BTC_USDT"), 1e-6);
        assertEquals(0.0, store.getTotalSellNotional("B-BTC_USDT"), 1e-9);
        assertEquals(1, store.getTradeCount("B-BTC_USDT"));
    }

    @Test
    void singleSellFillUpdatesAllFields() {
        store.applyFill("B-ETH_USDT", "sell", 10.0, 3400.0, 2000L, "TEST");

        assertEquals(-10.0, store.getNetQuantity("B-ETH_USDT"), 1e-9);
        assertEquals(0.0, store.getTotalBuyQuantity("B-ETH_USDT"), 1e-9);
        assertEquals(10.0, store.getTotalSellQuantity("B-ETH_USDT"), 1e-9);
        assertEquals(0.0, store.getTotalBuyNotional("B-ETH_USDT"), 1e-9);
        assertEquals(10.0 * 3400.0, store.getTotalSellNotional("B-ETH_USDT"), 1e-6);
        assertEquals(1, store.getTradeCount("B-ETH_USDT"));
    }

    @Test
    void multipleBuysAccumulate() {
        store.applyFill("B-BTC_USDT", "buy", 1.0, 65000.0, 1000L, "K");
        store.applyFill("B-BTC_USDT", "buy", 2.0, 65500.0, 2000L, "K");

        assertEquals(3.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(3.0, store.getTotalBuyQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(1.0 * 65000.0 + 2.0 * 65500.0, store.getTotalBuyNotional("B-BTC_USDT"), 1e-6);
        assertEquals(2, store.getTradeCount("B-BTC_USDT"));
    }

    @Test
    void multipleSellsAccumulate() {
        store.applyFill("B-ETH_USDT", "sell", 5.0, 3400.0, 1000L, "K");
        store.applyFill("B-ETH_USDT", "sell", 3.0, 3450.0, 2000L, "K");

        assertEquals(-8.0, store.getNetQuantity("B-ETH_USDT"), 1e-9);
        assertEquals(8.0, store.getTotalSellQuantity("B-ETH_USDT"), 1e-9);
        assertEquals(5.0 * 3400.0 + 3.0 * 3450.0, store.getTotalSellNotional("B-ETH_USDT"), 1e-6);
        assertEquals(2, store.getTradeCount("B-ETH_USDT"));
    }

    @Test
    void buysAndSellsNetCorrectly() {
        store.applyFill("B-BTC_USDT", "buy", 3.0, 65000.0, 1000L, "K");
        store.applyFill("B-BTC_USDT", "sell", 1.0, 65500.0, 2000L, "K");

        assertEquals(2.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(3.0, store.getTotalBuyQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(1.0, store.getTotalSellQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(2, store.getTradeCount("B-BTC_USDT"));
    }

    @Test
    void netQuantityCanGoNegative() {
        store.applyFill("B-BTC_USDT", "buy", 1.0, 65000.0, 1000L, "K");
        store.applyFill("B-BTC_USDT", "sell", 3.0, 65500.0, 2000L, "K");

        assertEquals(-2.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
    }

    @Test
    void caseInsensitiveSide() {
        store.applyFill("B-BTC_USDT", "BUY", 1.0, 65000.0, 1000L, "K");
        store.applyFill("B-BTC_USDT", "Buy", 2.0, 65000.0, 2000L, "K");

        assertEquals(3.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(3.0, store.getTotalBuyQuantity("B-BTC_USDT"), 1e-9);
    }

    @Test
    void sellSideIsCaseInsensitive() {
        store.applyFill("B-BTC_USDT", "SELL", 1.0, 65000.0, 1000L, "K");
        store.applyFill("B-BTC_USDT", "Sell", 2.0, 65000.0, 2000L, "K");

        assertEquals(-3.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(3.0, store.getTotalSellQuantity("B-BTC_USDT"), 1e-9);
    }

    @Test
    void multiplePairsAreIndependent() {
        store.applyFill("B-BTC_USDT", "buy", 1.0, 65000.0, 1000L, "K");
        store.applyFill("B-ETH_USDT", "sell", 5.0, 3400.0, 1000L, "K");

        assertEquals(1.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(-5.0, store.getNetQuantity("B-ETH_USDT"), 1e-9);
        assertEquals(0.0, store.getTotalSellQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(0.0, store.getTotalBuyQuantity("B-ETH_USDT"), 1e-9);
    }

    @Test
    void snapshotsReturnAllTrackedPairs() {
        store.applyFill("B-BTC_USDT", "buy", 1.0, 65000.0, 1000L, "K");
        store.applyFill("B-ETH_USDT", "sell", 5.0, 3400.0, 2000L, "K");
        store.applyFill("B-SOL_USDT", "buy", 100.0, 150.0, 3000L, "K");

        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();
        assertEquals(3, snapshots.size());

        for (ExposureSnapshot snap : snapshots) {
            assertNotNull(snap.getPair());
            assertTrue(snap.getSnapshotTs() > 0);
        }
    }

    @Test
    void snapshotReflectsCorrectValues() {
        store.applyFill("B-BTC_USDT", "buy", 2.0, 65000.0, 1000L, "K");
        store.applyFill("B-BTC_USDT", "sell", 0.5, 66000.0, 2000L, "K");

        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();
        ExposureSnapshot btcSnap = snapshots.stream()
                .filter(s -> "B-BTC_USDT".equals(s.getPair()))
                .findFirst()
                .orElseThrow();

        assertEquals(1.5, btcSnap.getNetQuantity(), 1e-9);
        assertEquals(2.0, btcSnap.getTotalBuyQuantity(), 1e-9);
        assertEquals(0.5, btcSnap.getTotalSellQuantity(), 1e-9);
        assertEquals(2.0 * 65000.0, btcSnap.getTotalBuyNotional(), 1e-6);
        assertEquals(0.5 * 66000.0, btcSnap.getTotalSellNotional(), 1e-6);
        assertEquals(2, btcSnap.getTradeCount());
        assertEquals(2000L, btcSnap.getLastUpdateTs());
    }

    @Test
    void emptyStoreReturnsEmptySnapshots() {
        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();
        assertTrue(snapshots.isEmpty());
    }

    @Test
    void printExposureSnapshotDoesNotThrow() {
        store.applyFill("B-BTC_USDT", "buy", 1.0, 65000.0, 1000L, "K");
        assertDoesNotThrow(() -> store.printExposureSnapshot());
    }

    @Test
    void printExposureSnapshotOnEmptyStoreDoesNotThrow() {
        assertDoesNotThrow(() -> store.printExposureSnapshot());
    }

    @Test
    void maxPairsExceededThrows() {
        for (int i = 0; i < 512; i++) {
            store.applyFill("PAIR-" + i, "buy", 0.1, 100.0, 1000L, "K");
        }
        assertThrows(IllegalStateException.class, () ->
                store.applyFill("PAIR-513", "buy", 0.1, 100.0, 1000L, "K"));
    }

    @Test
    void tradeCountIncrements() {
        for (int i = 0; i < 10; i++) {
            store.applyFill("B-BTC_USDT", "buy", 0.1, 65000.0, 1000L + i, "K");
        }
        assertEquals(10, store.getTradeCount("B-BTC_USDT"));
    }

    @Test
    void lastUpdateTimestampIsUpdated() {
        store.applyFill("B-BTC_USDT", "buy", 1.0, 65000.0, 1000L, "K");
        store.applyFill("B-BTC_USDT", "sell", 0.5, 66000.0, 5000L, "K");

        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();
        ExposureSnapshot snap = snapshots.get(0);
        assertEquals(5000L, snap.getLastUpdateTs());
    }

    @Test
    void notionalCalculation() {
        store.applyFill("B-BTC_USDT", "buy", 2.5, 64000.0, 1000L, "K");
        assertEquals(2.5 * 64000.0, store.getTotalBuyNotional("B-BTC_USDT"), 1e-6);

        store.applyFill("B-BTC_USDT", "sell", 1.5, 65000.0, 2000L, "K");
        assertEquals(1.5 * 65000.0, store.getTotalSellNotional("B-BTC_USDT"), 1e-6);
    }

    @Test
    void sourceLabelDoesNotAffectState() {
        store.applyFill("B-BTC_USDT", "buy", 1.0, 65000.0, 1000L, "KAFKA");
        store.applyFill("B-BTC_USDT", "buy", 1.0, 65000.0, 2000L, "AERON");

        assertEquals(2.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(2, store.getTradeCount("B-BTC_USDT"));
    }

    @Test
    void zeroQuantityFill() {
        store.applyFill("B-BTC_USDT", "buy", 0.0, 65000.0, 1000L, "K");

        assertEquals(0.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(0.0, store.getTotalBuyNotional("B-BTC_USDT"), 1e-9);
        assertEquals(1, store.getTradeCount("B-BTC_USDT"));
    }

    @Test
    void zeroPriceFill() {
        store.applyFill("B-BTC_USDT", "buy", 1.0, 0.0, 1000L, "K");

        assertEquals(1.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(0.0, store.getTotalBuyNotional("B-BTC_USDT"), 1e-9);
    }

    @Test
    void samePairAllocatesSingleSlot() {
        store.applyFill("B-BTC_USDT", "buy", 1.0, 65000.0, 1000L, "K");
        store.applyFill("B-BTC_USDT", "sell", 0.5, 66000.0, 2000L, "K");

        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();
        assertEquals(1, snapshots.size());
    }

    @Test
    void largeNumberOfFills() {
        double expectedNet = 0.0;
        for (int i = 0; i < 1000; i++) {
            String side = (i % 2 == 0) ? "buy" : "sell";
            double qty = 0.01 * (i + 1);
            store.applyFill("B-BTC_USDT", side, qty, 65000.0, 1000L + i, "K");
            expectedNet += (i % 2 == 0) ? qty : -qty;
        }

        assertEquals(expectedNet, store.getNetQuantity("B-BTC_USDT"), 1e-6);
        assertEquals(1000, store.getTradeCount("B-BTC_USDT"));
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void concurrentFillsAreSafe() throws InterruptedException {
        int threadCount = 8;
        int fillsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < fillsPerThread; i++) {
                        store.applyFill("B-BTC_USDT", "buy", 0.001, 65000.0,
                                1000L + threadId * fillsPerThread + i, "T" + threadId);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        assertEquals(threadCount * fillsPerThread, store.getTradeCount("B-BTC_USDT"));
        assertEquals(threadCount * fillsPerThread * 0.001,
                store.getNetQuantity("B-BTC_USDT"), 1e-6);
    }

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void concurrentFillsOnDifferentPairs() throws InterruptedException {
        int threadCount = 4;
        int fillsPerThread = 500;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        String[] pairs = {"B-BTC_USDT", "B-ETH_USDT", "B-SOL_USDT", "B-DOGE_USDT"};

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < fillsPerThread; i++) {
                        store.applyFill(pairs[threadId], "buy", 1.0, 100.0,
                                1000L + i, "T" + threadId);
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        latch.await();
        executor.shutdown();

        for (String pair : pairs) {
            assertEquals(fillsPerThread, store.getTradeCount(pair));
            assertEquals(fillsPerThread * 1.0, store.getNetQuantity(pair), 1e-6);
        }
    }

    @Test
    void nonBuySideTreatedAsSell() {
        store.applyFill("B-BTC_USDT", "something_else", 1.0, 65000.0, 1000L, "K");

        assertEquals(-1.0, store.getNetQuantity("B-BTC_USDT"), 1e-9);
        assertEquals(1.0, store.getTotalSellQuantity("B-BTC_USDT"), 1e-9);
    }

    @Test
    void snapshotTimestampIsReasonable() {
        store.applyFill("B-BTC_USDT", "buy", 1.0, 65000.0, 1000L, "K");

        long before = System.currentTimeMillis();
        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();
        long after = System.currentTimeMillis();

        ExposureSnapshot snap = snapshots.get(0);
        assertTrue(snap.getSnapshotTs() >= before);
        assertTrue(snap.getSnapshotTs() <= after);
    }

    @Test
    void manyPairsTrackedCorrectly() {
        int pairCount = 50;
        for (int i = 0; i < pairCount; i++) {
            store.applyFill("PAIR-" + i, "buy", 1.0 + i, 100.0 + i, 1000L + i, "K");
        }

        List<ExposureSnapshot> snapshots = store.getExposureSnapshots();
        assertEquals(pairCount, snapshots.size());

        for (int i = 0; i < pairCount; i++) {
            assertEquals(1.0 + i, store.getNetQuantity("PAIR-" + i), 1e-9);
        }
    }
}
