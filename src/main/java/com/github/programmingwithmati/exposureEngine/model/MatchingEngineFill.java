package com.github.programmingwithmati.exposureEngine.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class MatchingEngineFill {

    @JsonProperty("derivatives_futures_order_id")
    private String derivativesFuturesOrderId;

    @JsonProperty("quantity")
    private String quantity;

    @JsonProperty("price")
    private String price;

    @JsonProperty("pair")
    private String pair;

    @JsonProperty("exchange_trade_id")
    private String exchangeTradeId;

    @JsonProperty("side")
    private String side;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("is_maker")
    private boolean isMaker;
}
