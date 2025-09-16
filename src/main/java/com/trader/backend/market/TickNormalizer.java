package com.trader.backend.market;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

@Component
@Slf4j
public class TickNormalizer {

    private static final long MILLIS_THRESHOLD = 1_000_000_000_000L;

    public Optional<MarketDtos.NormalizedTick> normalize(String instrumentKey, JsonNode feed, JsonNode root) {
        if (feed == null || feed.isMissingNode() || feed.isNull()) {
            return Optional.empty();
        }

        JsonNode fullFeed = feed.path("fullFeed");
        JsonNode marketFeed = fullFeed.path("marketFF");
        boolean usingIndexFeed = false;
        if (marketFeed.isMissingNode() || marketFeed.isNull() || marketFeed.isEmpty()) {
            marketFeed = fullFeed.path("indexFF");
            usingIndexFeed = !marketFeed.isMissingNode() && !marketFeed.isNull() && !marketFeed.isEmpty();
        }

        if (marketFeed.isMissingNode() || marketFeed.isNull() || marketFeed.isEmpty()) {
            log.debug("No market feed for instrumentKey={}", instrumentKey);
            return Optional.empty();
        }

        JsonNode ltpc = marketFeed.path("ltpc");
        JsonNode ltpNode = ltpc.path("ltp");
        if (!ltpNode.isNumber()) {
            return Optional.empty();
        }

        double ltp = ltpNode.asDouble();
        long ts = extractTimestamp(feed, root);
        Double ltq = asDouble(ltpc.path("ltq"));
        Long ltt = asLong(ltpc.path("ltt"));
        Double cp = asDouble(ltpc.path("cp"));

        List<MarketDtos.BidAskQuote> bidAsk = usingIndexFeed ? List.of() : parseBidAsk(marketFeed.path("marketLevel").path("bidAskQuote"));
        Double oi = asDouble(marketFeed.path("oi"));
        MarketDtos.Greeks greeks = parseGreeks(marketFeed.path("optionGreeks"));
        MarketDtos.Ohlc ohlc = parseOhlc(marketFeed.path("marketOHLC").path("ohlc"));

        if (bidAsk != null && bidAsk.isEmpty()) {
            bidAsk = null;
        }
        if (greeks != null && greeks.delta() == null && greeks.theta() == null && greeks.gamma() == null
                && greeks.vega() == null && greeks.rho() == null) {
            greeks = null;
        }
        if (ohlc != null && ohlc.d1() == null && ohlc.i1() == null) {
            ohlc = null;
        }

        return Optional.of(new MarketDtos.NormalizedTick(
                "tick",
                ts,
                instrumentKey,
                ltp,
                ltq,
                ltt,
                cp,
                bidAsk,
                oi,
                greeks,
                ohlc
        ));
    }

    private List<MarketDtos.BidAskQuote> parseBidAsk(JsonNode quotesNode) {
        if (quotesNode == null || !quotesNode.isArray()) {
            return null;
        }
        List<MarketDtos.BidAskQuote> quotes = new ArrayList<>();
        for (JsonNode level : quotesNode) {
            if (quotes.size() >= 5) {
                break;
            }
            Double bidP = asDouble(level.path("bidP"));
            Double bidQ = asDouble(level.path("bidQ"));
            Double askP = asDouble(level.path("askP"));
            Double askQ = asDouble(level.path("askQ"));
            if (bidP == null && askP == null) {
                continue;
            }
            quotes.add(new MarketDtos.BidAskQuote(
                    bidP != null ? bidP : 0.0,
                    bidQ != null ? bidQ : 0.0,
                    askP != null ? askP : 0.0,
                    askQ != null ? askQ : 0.0
            ));
        }
        return quotes;
    }

    private MarketDtos.Greeks parseGreeks(JsonNode greeksNode) {
        if (greeksNode == null || greeksNode.isMissingNode() || greeksNode.isNull() || greeksNode.isEmpty()) {
            return null;
        }
        return new MarketDtos.Greeks(
                asDouble(greeksNode.path("delta")),
                asDouble(greeksNode.path("theta")),
                asDouble(greeksNode.path("gamma")),
                asDouble(greeksNode.path("vega")),
                asDouble(greeksNode.path("rho"))
        );
    }

    private MarketDtos.Ohlc parseOhlc(JsonNode ohlcNode) {
        if (ohlcNode == null || !ohlcNode.isArray() || ohlcNode.isEmpty()) {
            return null;
        }
        MarketDtos.OhlcEntry daily = null;
        MarketDtos.OhlcEntry intraday = null;
        for (JsonNode entry : ohlcNode) {
            String interval = entry.path("interval").asText("").toLowerCase(Locale.ROOT);
            MarketDtos.OhlcEntry normalized = new MarketDtos.OhlcEntry(
                    asDouble(entry.path("open")),
                    asDouble(entry.path("high")),
                    asDouble(entry.path("low")),
                    asDouble(entry.path("close")),
                    extractVolume(entry.path("vol")),
                    asLong(entry.path("ts"))
            );
            if (interval.isEmpty()) {
                continue;
            }
            if (interval.equals("1d") || interval.equals("d1") || interval.contains("day")) {
                daily = normalized;
            } else if (interval.equals("1m") || interval.equals("i1") || interval.contains("min")) {
                intraday = normalized;
            }
        }
        return new MarketDtos.Ohlc(daily, intraday);
    }

    private Object extractVolume(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.asLong();
        }
        if (node.isTextual()) {
            String txt = node.asText();
            try {
                return Long.parseLong(txt);
            } catch (NumberFormatException ignored) {
                return txt;
            }
        }
        return null;
    }

    private Double asDouble(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.asDouble();
        }
        if (node.isTextual()) {
            try {
                return Double.parseDouble(node.asText());
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private Long asLong(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            long value = node.asLong();
            return adjustEpoch(value);
        }
        if (node.isTextual()) {
            try {
                long value = Long.parseLong(node.asText());
                return adjustEpoch(value);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private long extractTimestamp(JsonNode feed, JsonNode root) {
        String[] ptrs = {"/fullFeed/marketFF/ts", "/fullFeed/ts", "/ts", "/timestamp"};
        for (String ptr : ptrs) {
            JsonNode node = feed.at(ptr);
            if (node.isNumber() || node.isTextual()) {
                Long value = asLong(node);
                if (value != null) {
                    return value;
                }
            }
        }
        JsonNode currentTs = root.path("currentTs");
        Long value = asLong(currentTs);
        if (value != null) {
            return value;
        }
        return Instant.now().toEpochMilli();
    }

    private long adjustEpoch(long value) {
        return value < MILLIS_THRESHOLD ? value * 1000L : value;
    }
}
