package com.trader.backend.market;

import com.trader.backend.service.CandleService;
import com.trader.backend.service.CandleService.Candle;
import com.trader.backend.service.CandleService.CandleResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

@RestController
@RequestMapping("/api/market")
@Slf4j
public class MarketHistoryController {

    private final CandleService candleService;

    public MarketHistoryController(ObjectProvider<CandleService> candleServiceProvider) {
        this.candleService = candleServiceProvider.getIfAvailable();
    }

    @GetMapping("/history")
    public Mono<MarketDtos.MarketHistoryResponse> history(@RequestParam("instrumentKey") String instrumentKey,
                                                          @RequestParam(value = "tf", defaultValue = "1m") String timeframe,
                                                          @RequestParam(value = "limit", defaultValue = "300") int limit) {
        if (!StringUtils.hasText(instrumentKey)) {
            return Mono.error(new IllegalArgumentException("instrumentKey is required"));
        }

        int sanitizedLimit = Math.min(Math.max(limit, 1), 1000);
        String normalizedTf = normalizeTf(timeframe);
        boolean influxSupported = !"1s".equalsIgnoreCase(timeframe);

        if (candleService == null || !influxSupported) {
            log.debug("Returning dummy history for instrumentKey={} tf={}", instrumentKey, timeframe);
            return Mono.just(dummyHistory(instrumentKey, timeframe, sanitizedLimit));
        }

        return candleService.fetchCandles(List.of(instrumentKey), normalizedTf, sanitizedLimit)
                .map(responses -> responses.isEmpty() ? null : responses.get(0))
                .flatMap(response -> {
                    if (response == null || response.candles().isEmpty()) {
                        log.debug("No historical candles available from Influx for instrumentKey={} tf={}", instrumentKey, normalizedTf);
                        return Mono.just(dummyHistory(instrumentKey, timeframe, sanitizedLimit));
                    }
                    return Mono.just(fromResponse(instrumentKey, timeframe, response));
                })
                .onErrorResume(ex -> {
                    log.warn("Falling back to dummy history for instrumentKey={} tf={} reason={}", instrumentKey, timeframe, ex.getMessage());
                    return Mono.just(dummyHistory(instrumentKey, timeframe, sanitizedLimit));
                });
    }

    private String normalizeTf(String tf) {
        String normalized = tf == null ? "1m" : tf.toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "5m" -> "5m";
            case "15m" -> "15m";
            case "3m" -> "3m";
            default -> "1m";
        };
    }

    private MarketDtos.MarketHistoryResponse fromResponse(String instrumentKey, String requestedTf, CandleResponse response) {
        List<MarketDtos.Candle> candles = new ArrayList<>(response.candles().size());
        for (Candle candle : response.candles()) {
            try {
                Instant ts = Instant.parse(candle.t());
                candles.add(new MarketDtos.Candle(
                        ts.toEpochMilli(),
                        candle.o(),
                        candle.h(),
                        candle.l(),
                        candle.c(),
                        candle.v()
                ));
            } catch (DateTimeParseException e) {
                log.debug("Skipping candle with unparsable timestamp: {}", candle.t());
            }
        }
        return new MarketDtos.MarketHistoryResponse(instrumentKey, requestedTf, candles);
    }

    private MarketDtos.MarketHistoryResponse dummyHistory(String instrumentKey, String timeframe, int limit) {
        Duration step = switch (timeframe.toLowerCase(Locale.ROOT)) {
            case "1s" -> Duration.ofSeconds(1);
            case "5m" -> Duration.ofMinutes(5);
            default -> Duration.ofMinutes(1);
        };
        List<MarketDtos.Candle> candles = new ArrayList<>(limit);
        Instant now = Instant.now();
        double price = 100.0 + ThreadLocalRandom.current().nextDouble(10.0);
        for (int i = limit; i >= 1; i--) {
            Instant ts = now.minus(step.multipliedBy(i));
            double drift = ThreadLocalRandom.current().nextDouble(-1.5, 1.5);
            double open = price;
            double close = Math.max(1.0, open + drift);
            double high = Math.max(open, close) + ThreadLocalRandom.current().nextDouble(0.5);
            double low = Math.min(open, close) - ThreadLocalRandom.current().nextDouble(0.5);
            double volume = 50 + ThreadLocalRandom.current().nextDouble(250.0);
            candles.add(new MarketDtos.Candle(ts.toEpochMilli(), open, high, low, close, volume));
            price = close;
        }
        return new MarketDtos.MarketHistoryResponse(instrumentKey, timeframe, candles);
    }
}
