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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/api/market")
@RequiredArgsConstructor
@Slf4j
public class MarketHistoryController {

    private static final ZoneId MARKET_ZONE = ZoneId.of("Asia/Kolkata");
    private static final Pattern INTERVAL_PATTERN =
            Pattern.compile("^(\\d+)?(minute|day)$", Pattern.CASE_INSENSITIVE);

    private final HistoricalDataService historicalDataService;

    @GetMapping("/history")
    public List<CandleDto> history(@RequestParam("instrumentKey") String instrumentKey,
                                   @RequestParam(value = "interval", defaultValue = "1minute") String interval,
                                   @RequestParam(value = "from", required = false)
                                   @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
                                   @RequestParam(value = "from_date", required = false)
                                   @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
                                   @RequestParam(value = "to", required = false)
                                   @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to,
                                   @RequestParam(value = "to_date", required = false)
                                   @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate) {
        if (!StringUtils.hasText(instrumentKey)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "instrumentKey is required");
        }

        String intervalToken = StringUtils.hasText(interval) ? interval.trim() : "1minute";
        Matcher matcher = INTERVAL_PATTERN.matcher(intervalToken);
        if (!matcher.matches() && intervalToken.chars().allMatch(Character::isDigit)) {
            intervalToken = intervalToken + "minute";
            matcher = INTERVAL_PATTERN.matcher(intervalToken);
        }
        if (!matcher.matches()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid interval: " + interval);
        }

        int parsedInterval = matcher.group(1) != null ? Integer.parseInt(matcher.group(1)) : 1;
        if (parsedInterval <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "interval must be positive");
        }

        String parsedUnit = matcher.group(2).toLowerCase(Locale.ROOT);
        if ("day".equals(parsedUnit) && parsedInterval != 1) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Daily interval must be 1day");
        }

        LocalDate today = LocalDate.now(MARKET_ZONE);
        LocalDate startDate = coalesceDate(from, fromDate, today);
        LocalDate endDate = coalesceDate(to, toDate, startDate);
        if (endDate.isBefore(startDate)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "to must be on or after from");
        }

        List<Candle> candles = historicalDataService.getOrFetchHistory(
                instrumentKey,
                parsedUnit,
                parsedInterval,
                startDate,
                endDate);
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

    public record CandleDto(long ts, double o, double h, double l, double c, long v) {
    }

    private LocalDate coalesceDate(LocalDate primary, LocalDate secondary, LocalDate fallback) {
        if (primary != null) {
            return primary;
        }
        if (secondary != null) {
            return secondary;
        }
        return fallback;
    }
}
