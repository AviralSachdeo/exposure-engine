package com.github.programmingwithmati.exposureEngine.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ExposureSnapshot {

    @JsonProperty("pair")
    private String pair;

    @JsonProperty("net_quantity")
    private double netQuantity;

    @JsonProperty("total_buy_quantity")
    private double totalBuyQuantity;

    @JsonProperty("total_sell_quantity")
    private double totalSellQuantity;

    @JsonProperty("total_buy_notional")
    private double totalBuyNotional;

    @JsonProperty("total_sell_notional")
    private double totalSellNotional;

    @JsonProperty("trade_count")
    private int tradeCount;

    @JsonProperty("last_update_ts")
    private long lastUpdateTs;

    @JsonProperty("snapshot_ts")
    private long snapshotTs;
}
