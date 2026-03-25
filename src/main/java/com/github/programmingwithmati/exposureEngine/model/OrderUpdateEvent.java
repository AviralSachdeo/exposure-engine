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
public class OrderUpdateEvent {

    @JsonProperty("e")
    private String eventType;

    @JsonProperty("E")
    private long eventTime;

    @JsonProperty("T")
    private long transactionTime;

    @JsonProperty("o")
    private OrderData order;

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class OrderData {

        @JsonProperty("s")
        private String symbol;

        @JsonProperty("c")
        private String clientOrderId;

        @JsonProperty("S")
        private String side;

        @JsonProperty("o")
        private String orderType;

        @JsonProperty("f")
        private String timeInForce;

        @JsonProperty("q")
        private String originalQuantity;

        @JsonProperty("p")
        private String originalPrice;

        @JsonProperty("ap")
        private String averagePrice;

        @JsonProperty("sp")
        private String stopPrice;

        @JsonProperty("x")
        private String executionType;

        @JsonProperty("X")
        private String orderStatus;

        @JsonProperty("i")
        private long orderId;

        @JsonProperty("l")
        private String lastFilledQuantity;

        @JsonProperty("z")
        private String filledAccumulatedQuantity;

        @JsonProperty("L")
        private String lastFilledPrice;

        @JsonProperty("N")
        private String commissionAsset;

        @JsonProperty("n")
        private String commission;

        @JsonProperty("t")
        private long tradeId;

        @JsonProperty("b")
        private String bidsNotional;

        @JsonProperty("a")
        private String askNotional;

        @JsonProperty("m")
        private boolean makerSide;

        @JsonProperty("R")
        private boolean reduceOnly;

        @JsonProperty("wt")
        private String workingType;

        @JsonProperty("ot")
        private String originalOrderType;

        @JsonProperty("ps")
        private String positionSide;

        @JsonProperty("cp")
        private boolean closeAll;

        @JsonProperty("AP")
        private String activationPrice;

        @JsonProperty("cr")
        private String callbackRate;

        @JsonProperty("pP")
        private boolean priceProtection;

        @JsonProperty("rp")
        private String realizedProfit;

        @JsonProperty("V")
        private String stpMode;

        @JsonProperty("pm")
        private String priceMatchMode;

        @JsonProperty("gtd")
        private long gtdAutoCancelTime;

        @JsonProperty("er")
        private String expiryReason;
    }
}
