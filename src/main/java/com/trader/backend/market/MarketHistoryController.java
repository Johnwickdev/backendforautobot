package com.trader.backend.market;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Locale;
import java.util.Set;

@RestController
@RequestMapping("/api/market")
@RequiredArgsConstructor
@Slf4j
public class MarketHistoryController {

    private static final ZoneId MARKET_ZONE = ZoneId.of("Asia/Kolkata");
    private static final Set<String> SUPPORTED_UNITS = Set.of("minutes", "hour", "day");

    private final HistoricalDataService historicalDataService;

    @GetMapping("/history")
    public List<CandleDto> history(@RequestParam("instrumentKey") String instrumentKey,
                                   @RequestParam(value = "unit", defaultValue = "minutes") String unit,
                                   @RequestParam(value = "interval", defaultValue = "1") int interval,
                                   @RequestParam(value = "from", required = false)
                                   @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
                                   @RequestParam(value = "to", required = false)
                                   @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to) {
        if (!StringUtils.hasText(instrumentKey)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "instrumentKey is required");
        }

        String normalizedUnit = normalizeUnit(unit);
        if (interval <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "interval must be positive");
        }

        LocalDate today = LocalDate.now(MARKET_ZONE);
        LocalDate startDate = from != null ? from : today;
        LocalDate endDate = to != null ? to : startDate;
        if (endDate.isBefore(startDate)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "to must be on or after from");
        }

        List<Candle> candles = historicalDataService.getOrFetchHistory(instrumentKey, normalizedUnit, interval, startDate, endDate);
        return candles.stream()
                .map(candle -> new CandleDto(
                        candle.getTs() != null ? candle.getTs().toEpochMilli() : 0L,
                        candle.getOpen(),
                        candle.getHigh(),
                        candle.getLow(),
                        candle.getClose(),
                        candle.getVolume()))
                .toList();
    }

    private String normalizeUnit(String unit) {
        String normalized = unit == null ? "minutes" : unit.toLowerCase(Locale.ROOT);
        if (!SUPPORTED_UNITS.contains(normalized)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unsupported unit");
        }
        return normalized;
    }

    public record CandleDto(long ts, double o, double h, double l, double c, long v) {
    }
}
