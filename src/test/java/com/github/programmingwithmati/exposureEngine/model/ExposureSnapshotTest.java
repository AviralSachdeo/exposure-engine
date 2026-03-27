package com.github.programmingwithmati.exposureEngine.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ExposureSnapshotTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void builderCreatesSnapshot() {
        ExposureSnapshot snap = ExposureSnapshot.builder()
                .pair("B-BTC_USDT")
                .netQuantity(1.5)
                .totalBuyQuantity(3.0)
                .totalSellQuantity(1.5)
                .totalBuyNotional(195000.0)
                .totalSellNotional(99000.0)
                .tradeCount(5)
                .lastUpdateTs(1000L)
                .snapshotTs(2000L)
                .build();

        assertEquals("B-BTC_USDT", snap.getPair());
        assertEquals(1.5, snap.getNetQuantity());
        assertEquals(3.0, snap.getTotalBuyQuantity());
        assertEquals(1.5, snap.getTotalSellQuantity());
        assertEquals(195000.0, snap.getTotalBuyNotional());
        assertEquals(99000.0, snap.getTotalSellNotional());
        assertEquals(5, snap.getTradeCount());
        assertEquals(1000L, snap.getLastUpdateTs());
        assertEquals(2000L, snap.getSnapshotTs());
    }

    @Test
    void noArgConstructor() {
        ExposureSnapshot snap = new ExposureSnapshot();
        assertNull(snap.getPair());
        assertEquals(0.0, snap.getNetQuantity());
        assertEquals(0, snap.getTradeCount());
    }

    @Test
    void allArgConstructor() {
        ExposureSnapshot snap = new ExposureSnapshot(
                "B-ETH_USDT", 5.0, 10.0, 5.0, 34000.0, 17250.0, 3, 500L, 600L);

        assertEquals("B-ETH_USDT", snap.getPair());
        assertEquals(5.0, snap.getNetQuantity());
    }

    @Test
    void settersWork() {
        ExposureSnapshot snap = new ExposureSnapshot();
        snap.setPair("B-SOL_USDT");
        snap.setNetQuantity(100.0);
        snap.setTotalBuyQuantity(200.0);
        snap.setTotalSellQuantity(100.0);
        snap.setTotalBuyNotional(30000.0);
        snap.setTotalSellNotional(16000.0);
        snap.setTradeCount(10);
        snap.setLastUpdateTs(3000L);
        snap.setSnapshotTs(4000L);

        assertEquals("B-SOL_USDT", snap.getPair());
        assertEquals(100.0, snap.getNetQuantity());
        assertEquals(200.0, snap.getTotalBuyQuantity());
        assertEquals(100.0, snap.getTotalSellQuantity());
        assertEquals(30000.0, snap.getTotalBuyNotional());
        assertEquals(16000.0, snap.getTotalSellNotional());
        assertEquals(10, snap.getTradeCount());
        assertEquals(3000L, snap.getLastUpdateTs());
        assertEquals(4000L, snap.getSnapshotTs());
    }

    @Test
    void jsonSerializationRoundTrip() throws Exception {
        ExposureSnapshot original = ExposureSnapshot.builder()
                .pair("B-BTC_USDT")
                .netQuantity(1.5)
                .totalBuyQuantity(3.0)
                .totalSellQuantity(1.5)
                .totalBuyNotional(195000.0)
                .totalSellNotional(99000.0)
                .tradeCount(5)
                .lastUpdateTs(1000L)
                .snapshotTs(2000L)
                .build();

        String json = MAPPER.writeValueAsString(original);
        ExposureSnapshot deserialized = MAPPER.readValue(json, ExposureSnapshot.class);

        assertEquals(original, deserialized);
    }

    @Test
    void jsonPropertyNamesCorrect() throws Exception {
        ExposureSnapshot snap = ExposureSnapshot.builder()
                .pair("B-BTC_USDT")
                .netQuantity(1.5)
                .totalBuyQuantity(3.0)
                .totalSellQuantity(1.5)
                .totalBuyNotional(195000.0)
                .totalSellNotional(99000.0)
                .tradeCount(5)
                .lastUpdateTs(1000L)
                .snapshotTs(2000L)
                .build();

        String json = MAPPER.writeValueAsString(snap);

        assertTrue(json.contains("\"pair\""));
        assertTrue(json.contains("\"net_quantity\""));
        assertTrue(json.contains("\"total_buy_quantity\""));
        assertTrue(json.contains("\"total_sell_quantity\""));
        assertTrue(json.contains("\"total_buy_notional\""));
        assertTrue(json.contains("\"total_sell_notional\""));
        assertTrue(json.contains("\"trade_count\""));
        assertTrue(json.contains("\"last_update_ts\""));
        assertTrue(json.contains("\"snapshot_ts\""));
    }

    @Test
    void jsonDeserializationFromSnakeCase() throws Exception {
        String json = """
                {
                    "pair": "B-ETH_USDT",
                    "net_quantity": -5.0,
                    "total_buy_quantity": 10.0,
                    "total_sell_quantity": 15.0,
                    "total_buy_notional": 34000.0,
                    "total_sell_notional": 51750.0,
                    "trade_count": 8,
                    "last_update_ts": 9999,
                    "snapshot_ts": 10000
                }
                """;

        ExposureSnapshot snap = MAPPER.readValue(json, ExposureSnapshot.class);

        assertEquals("B-ETH_USDT", snap.getPair());
        assertEquals(-5.0, snap.getNetQuantity());
        assertEquals(10.0, snap.getTotalBuyQuantity());
        assertEquals(15.0, snap.getTotalSellQuantity());
        assertEquals(34000.0, snap.getTotalBuyNotional());
        assertEquals(51750.0, snap.getTotalSellNotional());
        assertEquals(8, snap.getTradeCount());
        assertEquals(9999L, snap.getLastUpdateTs());
        assertEquals(10000L, snap.getSnapshotTs());
    }

    @Test
    void equalsAndHashCode() {
        ExposureSnapshot snap1 = ExposureSnapshot.builder()
                .pair("B-BTC_USDT").netQuantity(1.0).tradeCount(1).build();
        ExposureSnapshot snap2 = ExposureSnapshot.builder()
                .pair("B-BTC_USDT").netQuantity(1.0).tradeCount(1).build();
        ExposureSnapshot snap3 = ExposureSnapshot.builder()
                .pair("B-ETH_USDT").netQuantity(2.0).tradeCount(2).build();

        assertEquals(snap1, snap2);
        assertEquals(snap1.hashCode(), snap2.hashCode());
        assertNotEquals(snap1, snap3);
    }

    @Test
    void toStringContainsPair() {
        ExposureSnapshot snap = ExposureSnapshot.builder().pair("B-BTC_USDT").build();
        assertTrue(snap.toString().contains("B-BTC_USDT"));
    }
}
