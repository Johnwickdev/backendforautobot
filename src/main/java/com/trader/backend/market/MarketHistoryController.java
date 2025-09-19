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

    @GetMapping("/history")

    public List<CandleDto> history(@RequestParam("instrumentKey") String instrumentKey,
                                   @RequestParam(value = "interval", defaultValue = "1minute") String interval,
                                   @RequestParam("toDate") String toDate,
                                   @RequestParam(value = "fromDate", required = false) String fromDate) {
        if (!StringUtils.hasText(instrumentKey)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "instrumentKey is required");
        }

        String intervalToken = StringUtils.hasText(interval) ? interval.trim() : "1minute";
        Matcher matcher = INTERVAL_PATTERN.matcher(intervalToken);
        if (!matcher.matches()) {
            if (intervalToken.chars().allMatch(Character::isDigit)) {
                intervalToken = intervalToken + "minute";
                matcher = INTERVAL_PATTERN.matcher(intervalToken);
            }
        }
        if (!matcher.matches()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid interval: " + intervalToken);
        }

        int units = matcher.group(1) != null ? Integer.parseInt(matcher.group(1)) : 1;
        if (units <= 0) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "interval must be positive");
        }

        String unit = matcher.group(2).toLowerCase(Locale.ROOT);
        if ("day".equals(unit) && matcher.group(1) == null) {
            units = 1;
        }

        String normalizedUnit = unit;
        int normalizedInterval = units;

        LocalDate endDate = parseDate(toDate, "toDate");
        LocalDate startDate = StringUtils.hasText(fromDate) ? parseDate(fromDate, "fromDate") : endDate;
        if (endDate.isBefore(startDate)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "to must be on or after from");
        }

        List<Candle> candles = historicalDataService.getOrFetchHistory(
                instrumentKey,
                normalizedUnit,
                normalizedInterval,
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
