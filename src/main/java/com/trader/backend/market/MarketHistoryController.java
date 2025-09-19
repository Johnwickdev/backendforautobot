package com.trader.backend.market;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/api/market")
@RequiredArgsConstructor
public class MarketHistoryController {

    private static final Pattern INTERVAL_PATTERN =
            Pattern.compile("^(\\d+)?(minute|day)$", Pattern.CASE_INSENSITIVE);

    private static final ZoneId MARKET_ZONE = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    private final HistoricalDataService historicalDataService;

    @GetMapping("/history")
    public List<CandleDto> history(@RequestParam("instrumentKey") String instrumentKey,
                                   @RequestParam(value = "interval", defaultValue = "1minute") String interval,
                                   @RequestParam(value = "toDate", required = false) String toDate,
                                   @RequestParam(value = "fromDate", required = false) String fromDate) {

        if (!StringUtils.hasText(instrumentKey)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "instrumentKey is required");
        }

        // ---- interval parsing: "1minute" | "5minute" | "day" | "1" -> "1minute"
        String intervalToken = StringUtils.hasText(interval) ? interval.trim() : "1minute";
        Matcher matcher = INTERVAL_PATTERN.matcher(intervalToken);
        if (!matcher.matches()) {
            // allow bare numbers like "1", "5" -> treat as minutes
            if (intervalToken.chars().allMatch(Character::isDigit)) {
                intervalToken = intervalToken + "minute";
                matcher = INTERVAL_PATTERN.matcher(intervalToken);
            }
        }
        if (!matcher.matches()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    "Invalid interval. Use one of: 1minute, 5minute, day (or a number like 1/5 which means minutes)");
        }

        int units = matcher.group(1) != null ? Integer.parseInt(matcher.group(1)) : 1;
        if (units <= 0) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "interval must be positive");

        String unit = matcher.group(2).toLowerCase(Locale.ROOT);
        if ("day".equals(unit) && matcher.group(1) == null) units = 1;

        String normalizedUnit = unit;
        int normalizedInterval = units;

        // ---- smart defaults for dates
        LocalDate today = LocalDate.now(MARKET_ZONE);
        LocalDate endDate = StringUtils.hasText(toDate) ? parseDate(toDate, "toDate") : today;

        boolean isDerivative = looksLikeDerivative(instrumentKey);
        LocalDate defaultFrom = isDerivative ? endDate.minusDays(45)    // “current expiry” window
                : endDate.minusYears(10);  // 10 years for equities

        LocalDate startDate = StringUtils.hasText(fromDate) ? parseDate(fromDate, "fromDate") : defaultFrom;

        if (endDate.isBefore(startDate)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "toDate must be on or after fromDate");
        }

        List<Candle> candles = historicalDataService.getOrFetchHistory(
                instrumentKey, normalizedUnit, normalizedInterval, startDate, endDate);

        return candles.stream()
                .map(c -> new CandleDto(
                        c.getTs() != null ? c.getTs().toEpochMilli() : 0L,
                        c.getOpen(), c.getHigh(), c.getLow(), c.getClose(), c.getVolume()))
                .toList();
    }

    private static boolean looksLikeDerivative(String key) {
        if (key == null) return false;
        String k = key.toUpperCase(Locale.ROOT);
        // Heuristics covering common Upstox derivative instrument keys
        return k.contains("_FUT")
                || k.contains("|FUT")
                || k.contains("FUTIDX")
                || k.contains("FUTSTK")
                || k.endsWith("CE")
                || k.endsWith("PE")
                || k.contains("_CE") || k.contains("_PE")
                || k.contains("|OPT");
    }

    private LocalDate parseDate(String value, String paramName) {
        try {
            return LocalDate.parse(value, DATE_FORMATTER);
        } catch (DateTimeParseException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    paramName + " must be in YYYY-MM-DD format");
        }
    }

    public record CandleDto(long ts, double o, double h, double l, double c, long v) { }
}
