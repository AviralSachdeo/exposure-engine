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
public class OrderUpdateKafkaPayload {

    @JsonProperty("derivatives_futures_order_id")
    private String derivativesFuturesOrderId;

    @JsonProperty("quantity")
    private String quantity;

    @JsonProperty("price")
    private String price;

    @JsonProperty("pair")
    private String pair;

    @JsonProperty("exchange_trade_id")
    private long exchangeTradeId;

    @JsonProperty("ecode")
    private String ecode;

    @JsonProperty("order_status")
    private String orderStatus;

    @JsonProperty("timestamp")
    private long timestamp;

    @JsonProperty("order_filled_quantity")
    private String orderFilledQuantity;

    @JsonProperty("order_avg_price")
    private String orderAvgPrice;

    @JsonProperty("side")
    private String side;

    @JsonProperty("is_maker")
    private boolean isMaker;

    /**
     * Maps a Binance ORDER_TRADE_UPDATE event to the Kafka payload format.
     * Converts the Binance symbol (e.g. "BTCUSDT") to the internal
     * instrument pair format (e.g. "B-BTC_USDT").
     */
    public static OrderUpdateKafkaPayload fromOrderUpdateEvent(OrderUpdateEvent event) {
        OrderUpdateEvent.OrderData o = event.getOrder();

        return OrderUpdateKafkaPayload.builder()
                .derivativesFuturesOrderId(o.getClientOrderId())
                .quantity(o.getLastFilledQuantity())
                .price(o.getLastFilledPrice())
                .pair(toInstrumentPair(o.getSymbol()))
                .exchangeTradeId(o.getTradeId())
                .ecode("B")
                .orderStatus(o.getOrderStatus())
                .timestamp(event.getTransactionTime())
                .orderFilledQuantity(o.getFilledAccumulatedQuantity())
                .orderAvgPrice(o.getAveragePrice())
                .side(o.getSide().toLowerCase())
                .isMaker(o.isMakerSide())
                .build();
    }

    /**
     * Converts a Binance symbol like "BTCUSDT" to the internal pair format "B-BTC_USDT".
     * Strips known quote currencies (USDT, BUSD, USDC, BTC, ETH) from the end.
     */
    private static String toInstrumentPair(String binanceSymbol) {
        String[] quoteCurrencies = {"USDT", "BUSD", "USDC", "BTC", "ETH"};
        for (String quote : quoteCurrencies) {
            if (binanceSymbol.endsWith(quote)) {
                String base = binanceSymbol.substring(0, binanceSymbol.length() - quote.length());
                return "B-" + base + "_" + quote;
            }
        }
        return "B-" + binanceSymbol;
    }
}
