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

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    private final HistoricalDataService historicalDataService;

    // MarketHistoryController.java
@GetMapping("/history")
public List<CandleDto> history(
    @RequestParam String instrumentKey,
    @RequestParam(defaultValue = "1minute") String interval,
    @RequestParam(required = false) String toDate,
    @RequestParam(required = false) String fromDate,
    @RequestParam(required = false) Integer limit
) {
    if (!StringUtils.hasText(instrumentKey)) {
        throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "instrumentKey is required");
    }

    // Parse interval like 1minute, 5minute, or day
    String intervalToken = (StringUtils.hasText(interval) ? interval.trim() : "1minute");
    Matcher m = INTERVAL_PATTERN.matcher(intervalToken);
    if (!m.matches()) {
        if (intervalToken.chars().allMatch(Character::isDigit)) {
            intervalToken += "minute";
            m = INTERVAL_PATTERN.matcher(intervalToken);
        }
    }
    if (!m.matches()) {
        throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid interval: " + intervalToken);
    }
    int units = m.group(1) != null ? Integer.parseInt(m.group(1)) : 1;
    if (units <= 0) {
        throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "interval must be positive");
    }
    String unit = m.group(2).toLowerCase(Locale.ROOT);
    if ("day".equals(unit) && m.group(1) == null) {
        units = 1;
    }

    // Default toDate to today if not provided
    LocalDate endDate = (StringUtils.hasText(toDate) ? parseDate(toDate, "toDate") : LocalDate.now());
    LocalDate startDate;
    if (limit != null && limit > 0) {
        // Use limit (number of candles) to compute fromDate
        int minutesPerCandle = "day".equals(unit) ? 1440 : units;
        startDate = endDate.minusDays((long) limit * minutesPerCandle / 1440);
    } else {
        // If fromDate provided, parse it; otherwise use endDate
        startDate = StringUtils.hasText(fromDate) ? parseDate(fromDate, "fromDate") : endDate;
        if (endDate.isBefore(startDate)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "to must be on or after from");
        }
    }

    // Call HistoricalDataService to fetch or retrieve cached candles
    List<Candle> candles = historicalDataService.getOrFetchHistory(
        instrumentKey,
        unit,
        units,
        startDate,
        endDate
    );

    return candles.stream().map(c -> new CandleDto(
        c.getTs() != null ? c.getTs().toEpochMilli() : 0L,
        c.getOpen(),
        c.getHigh(),
        c.getLow(),
        c.getClose(),
        c.getVolume()
    )).toList();
}

    private LocalDate parseDate(String value, String paramName) {
        if (!StringUtils.hasText(value)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    paramName + " is required");
        }
        try {
            return LocalDate.parse(value, DATE_FORMATTER);
        } catch (DateTimeParseException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST,
                    paramName + " must be in YYYY-MM-DD format");
        }
    }

    public record CandleDto(long ts, double o, double h, double l, double c, long v) {
    }
}
