package com.trader.backend.market;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

public final class MarketDtos {

    private MarketDtos() {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record NormalizedTick(
            String type,
            long ts,
            String instrumentKey,
            double ltp,
            Double ltq,
            Long ltt,
            Double cp,
            List<BidAskQuote> bidAsk,
            Double oi,
            Greeks greeks,
            Ohlc ohlc
    ) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record BidAskQuote(double bidP, double bidQ, double askP, double askQ) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Greeks(Double delta, Double theta, Double gamma, Double vega, Double rho) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Ohlc(OhlcEntry d1, OhlcEntry i1) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record OhlcEntry(Double o, Double h, Double l, Double c, Object v, Long ts) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record MarketHistoryResponse(String instrumentKey, String tf, List<Candle> candles) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Candle(long ts, double o, double h, double l, double c, double v) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record InstrumentHit(
            String display,
            String instrumentKey,
            String segment,
            String symbol,
            Long expiry,
            Integer strikePrice,
            String instrumentType
    ) {
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record RefreshResult(int processed, int upserted, int skipped) {
    }
}
