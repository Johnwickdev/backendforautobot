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
    public List<CandleDto> history(
            @RequestParam String instrumentKey,
            @RequestParam(defaultValue = "1minute") String interval,
            @RequestParam(required = false) String toDate,        // now optional
            @RequestParam(required = false) String fromDate,      // optional
            @RequestParam(required = false, defaultValue = "300") Integer limit // legacy support
    ) {
        if (!StringUtils.hasText(instrumentKey)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "instrumentKey is required");
        }

        // parse interval like "1minute" / "5minute" / "day"
        Matcher m = INTERVAL_PATTERN.matcher(StringUtils.hasText(interval) ? interval.trim() : "1minute");
        if (!m.matches() && interval.chars().allMatch(Character::isDigit)) {
            m = INTERVAL_PATTERN.matcher(interval + "minute");
        }
        if (!m.matches()) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid interval: " + interval);
        int units = m.group(1) != null ? Integer.parseInt(m.group(1)) : 1;
        if (units <= 0) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "interval must be positive");
        String unit = m.group(2).toLowerCase(Locale.ROOT);
        if ("day".equals(unit) && m.group(1) == null) units = 1;

        // Dates: default to today IST if missing
        LocalDate endDate = StringUtils.hasText(toDate)
                ? parseDate(toDate, "toDate")
                : LocalDate.now(ZoneId.of("Asia/Kolkata"));

        LocalDate startDate;
        if (StringUtils.hasText(fromDate)) {
            startDate = parseDate(fromDate, "fromDate");
        } else {
            // Derive from limit (fallback for old frontend). Cap to 10 years.
            int n = (limit != null && limit > 0) ? limit : 300;
            // crude span: n * units of the chosen unit
            switch (unit) {
                case "minute" -> startDate = endDate.minusDays(Math.min(3650, Math.max(1, (n * units) / (60 * 6) + 1))); // ~6 trading hours/day
                case "day"    -> startDate = endDate.minusDays(Math.min(3650, n * units));
                default       -> startDate = endDate.minusDays(Math.min(3650, n)); // safe fallback
            }
        }
        if (endDate.isBefore(startDate)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "toDate must be on or after fromDate");
        }

        List<Candle> candles = historicalDataService.getOrFetchHistory(
                instrumentKey, unit, units, startDate, endDate);

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
