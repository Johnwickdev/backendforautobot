package com.trader.backend.market;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
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

    private static final ZoneId MARKET_ZONE = ZoneId.of("Asia/Kolkata");
    private static final Pattern INTERVAL_PATTERN =
            Pattern.compile("^(\\d+)?(minute|day)$", Pattern.CASE_INSENSITIVE);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    private final HistoricalDataService historicalDataService;

    @GetMapping("/history")
    public List<CandleDto> history(
            @RequestParam String instrumentKey,
            @RequestParam(defaultValue = "1minute") String interval,
            @RequestParam(required = false) String toDate,
            @RequestParam(required = false) String fromDate
    ) {

        if (!StringUtils.hasText(instrumentKey)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "instrumentKey is required");
        }

        // -------- interval parsing: 1minute / 5minute / day (default 1minute)
        String token = StringUtils.hasText(interval) ? interval.trim() : "1minute";
        Matcher m = INTERVAL_PATTERN.matcher(token);
        if (!m.matches()) {
            if (token.chars().allMatch(Character::isDigit)) {
                token = token + "minute";
                m = INTERVAL_PATTERN.matcher(token);
            }
        }
        if (!m.matches()) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Invalid interval: " + interval);

        int units = (m.group(1) != null) ? Integer.parseInt(m.group(1)) : 1;
        if (units <= 0) throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "interval must be positive");
        String unit = m.group(2).toLowerCase(Locale.ROOT);
        if ("day".equals(unit) && m.group(1) == null) units = 1;

        // -------- date defaults: to=today (IST), from=to-10y
        LocalDate todayIst = LocalDate.now(MARKET_ZONE);
        LocalDate to = parseOrDefault(toDate, todayIst);
        LocalDate from = parseOrDefault(fromDate, to.minusYears(10));
        if (to.isBefore(from)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "toDate must be on/after fromDate");
        }

        var candles = historicalDataService.getOrFetchHistory(
                instrumentKey, unit, units, from, to
        );

        return candles.stream()
                .map(c -> new CandleDto(
                        c.getTs() != null ? c.getTs().toEpochMilli() : 0L,
                        c.getOpen(), c.getHigh(), c.getLow(), c.getClose(), c.getVolume()
                ))
                .toList();
    }

    private LocalDate parseOrDefault(String value, LocalDate def) {
        if (!StringUtils.hasText(value)) return def;
        try {
            return LocalDate.parse(value, DATE_FORMATTER);
        } catch (DateTimeParseException ex) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Dates must be YYYY-MM-DD");
        }
    }

    public record CandleDto(long ts, double o, double h, double l, double c, long v) { }
}
