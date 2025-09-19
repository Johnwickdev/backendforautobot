package com.trader.backend.market;

import com.fasterxml.jackson.databind.JsonNode;
import com.trader.backend.service.UpstoxAuthService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * V3-compliant historical candle fetcher with window chunking that respects Upstox limits.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class HistoricalDataService {

    private static final ZoneId MARKET_ZONE = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter LEGACY_TS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").withZone(MARKET_ZONE);
    private static final Duration API_TIMEOUT = Duration.ofSeconds(15);

    private final WebClient.Builder webClientBuilder;
    private final UpstoxAuthService upstoxAuthService;
    private final CandleRepository candleRepository;

    /**
     * Fetch (and cache) candles between from..to inclusive, merging existing cache with new data.
     * Unit can be "minute"/"hour"/"day"/"week"/"month" (controller already validates).
     */
    public List<Candle> getOrFetchHistory(String instrumentKey,
                                          String unit,
                                          int interval,
                                          LocalDate from,
                                          LocalDate to) {

        String u = normalizeUnit(unit);            // minute/hour/day/week/month
        String v3Unit = toV3UnitPlural(u);         // minutes/hours/days/weeks/months
        LocalDate start = Objects.requireNonNull(from, "from");
        LocalDate end   = Objects.requireNonNull(to,   "to");

        // read cache first
        Instant fromTs = start.atStartOfDay(MARKET_ZONE).toInstant();
        Instant toTs   = end.plusDays(1).atStartOfDay(MARKET_ZONE).minusNanos(1).toInstant();
        List<Candle> cached = candleRepository
                .findByInstrumentKeyAndUnitAndIntervalAndTsBetween(instrumentKey, u, interval, fromTs, toTs);

        // ensure token
        if (!ensureToken()) {
            log.warn("Token not ready. Serving cached {} candles.", cached.size());
            return sortByTimestamp(cached);
        }
        String accessToken = upstoxAuthService.getAccessToken();
        if (accessToken == null || accessToken.isBlank()) {
            log.warn("No access token. Serving cached {} candles.", cached.size());
            return sortByTimestamp(cached);
        }

        // chunk window to respect V3 limits
        int chunkDays = windowDaysFor(u, interval);
        List<Candle> fetched = new ArrayList<>();

        LocalDate cursorTo = end;
        while (!cursorTo.isBefore(start)) {
            LocalDate cursorFrom = cursorTo.minusDays(chunkDays - 1L);
            if (cursorFrom.isBefore(start)) cursorFrom = start;

            List<Candle> part = fetchWindowFromUpstox(
                    instrumentKey, v3Unit, interval, cursorFrom, cursorTo, accessToken, u);

            if (!CollectionUtils.isEmpty(part)) {
                candleRepository.saveAll(part);
                fetched.addAll(part);
            }

            // move backward one day to avoid overlap
            cursorTo = cursorFrom.minusDays(1);
        }

        // merge cache + fetched (by id) and sort
        Map<String, Candle> map = new HashMap<>();
        for (Candle c : cached)   map.put(c.getId(), c);
        for (Candle c : fetched)  map.put(c.getId(), c);

        return sortByTimestamp(new ArrayList<>(map.values()));
    }

    /**
     * Calls Upstox V3 historical candle API for a single window.
     * Endpoint (V3): /historical-candle/{instrument_key}/{unit}/{interval}/{to_date}/{from_date}
     *
     * NOTE: unit must be plural in V3 (minutes/hours/days/weeks/months).
     */
    private List<Candle> fetchWindowFromUpstox(String instrumentKey,
                                               String v3UnitPlural,
                                               int interval,
                                               LocalDate from,
                                               LocalDate to,
                                               String accessToken,
                                               String ourUnitSingularForStorage) {
        try {
            // Important: pass all 5 segments as PATH segments (no query params for dates in V3)
            URI uri = UriComponentsBuilder
                    .fromHttpUrl("https://api.upstox.com/v3/historical-candle")
                    .pathSegment(instrumentKey)                                // will encode '|' -> %7C
                    .pathSegment(v3UnitPlural)                                 // minutes/hours/days/...
                    .pathSegment(String.valueOf(interval))
                    .pathSegment(DATE_FMT.format(to))                          // to_date (inclusive)
                    .pathSegment(DATE_FMT.format(from))                        // from_date
                    .build()
                    .encode()
                    .toUri();

            WebClient client = webClientBuilder.clone().build();

            JsonNode resp = client.get()
                    .uri(uri)
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .block(API_TIMEOUT);

            JsonNode rows = resp == null ? null : resp.path("data").path("candles");
            if (rows == null || !rows.isArray()) {
                log.warn("Unexpected historical payload for {} {} {} {}..{} : {}",
                        instrumentKey, v3UnitPlural, interval, from, to, resp);
                return List.of();
            }

            List<Candle> out = new ArrayList<>();
            for (JsonNode row : rows) {
                if (!row.isArray() || row.size() < 5) continue;

                Instant ts = parseTimestamp(row.get(0).asText(null));
                if (ts == null) continue;

                double o = row.get(1).asDouble();
                double h = row.get(2).asDouble();
                double l = row.get(3).asDouble();
                double c = row.get(4).asDouble();
                long v   = row.size() > 5 && row.get(5).isNumber() ? row.get(5).longValue() : 0L;
                long oi  = row.size() > 6 && row.get(6).isNumber() ? row.get(6).longValue() : 0L;

                // store with our normalized singular unit (minute/hour/day/week/month) for consistency
                out.add(Candle.create(instrumentKey, ourUnitSingularForStorage, interval, ts, o, h, l, c, v, oi));
            }
            return out;

        } catch (Exception e) {
            log.warn("Upstox V3 history fetch failed for {} {} {} {}..{}",
                    instrumentKey, v3UnitPlural, interval, from, to, e);
            return List.of();
        }
    }

    /** Choose safe window size (days) per V3 doc limits. */
    private int windowDaysFor(String unitSingular, int interval) {
        switch (unitSingular) {
            case "minute":
                // V3: 1 month for <=15m, 1 quarter for >15m
                return (interval <= 15) ? 30 : 90;
            case "hour":
                // V3: 1 quarter (≈90 days)
                return 90;
            case "day":
                // V3: up to 1 decade leading to to_date (we chunk in 3650-day slabs to be safe)
                return 3650;
            case "week":
                // V3: no limit; chunk generously to keep responses reasonable
                return 3650; // ~10 years
            case "month":
                // V3: no limit; chunk generously
                return 3650 * 3; // ~30 years
            default:
                // fallback
                return 365;
        }
    }

    /** Controller normalizes to minute/hour/day/week/month. */
    private String normalizeUnit(String unit) {
        String u = unit == null ? "minute" : unit.toLowerCase(Locale.ROOT);
        return switch (u) {
            case "minute", "minutes" -> "minute";
            case "hour", "hours"     -> "hour";
            case "day", "days"       -> "day";
            case "week", "weeks"     -> "week";
            case "month", "months"   -> "month";
            default -> throw new IllegalArgumentException("Unsupported unit: " + unit);
        };
    }

    /** V3 requires plural unit names. */
    private String toV3UnitPlural(String unitSingular) {
        return switch (unitSingular) {
            case "minute" -> "minutes";
            case "hour"   -> "hours";
            case "day"    -> "days";
            case "week"   -> "weeks";
            case "month"  -> "months";
            default       -> unitSingular; // should not happen
        };
    }

    private boolean ensureToken() {
        try {
            Mono<Boolean> mono = upstoxAuthService.ensureValidToken();
            Boolean ok = mono == null ? Boolean.FALSE : mono.block(API_TIMEOUT);
            return Boolean.TRUE.equals(ok);
        } catch (Exception e) {
            log.warn("Failed to ensure Upstox token is valid", e);
            return false;
        }
    }

    private Instant parseTimestamp(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException ignored) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(value, LEGACY_TS);
                return ldt.atZone(MARKET_ZONE).toInstant();
            } catch (DateTimeParseException dtpe) {
                log.debug("Unable to parse candle timestamp: {}", value);
                return null;
            }
        }
    }

    private List<Candle> sortByTimestamp(List<Candle> candles) {
        candles.sort(Comparator.comparing(Candle::getTs));
        return candles;
    }

    // (Optional) kept for any callers using expected counts; not strictly required for V3 flow
    @SuppressWarnings("unused")
    private long expectedCount(String unit, int interval, LocalDate from, LocalDate to) {
        if (interval <= 0 || to.isBefore(from)) return 0;
        ChronoUnit cu = switch (unit) {
            case "minute" -> ChronoUnit.MINUTES;
            case "hour"   -> ChronoUnit.HOURS;
            case "day"    -> ChronoUnit.DAYS;
            case "week"   -> ChronoUnit.WEEKS;
            case "month"  -> ChronoUnit.MONTHS;
            default -> throw new IllegalArgumentException("Unsupported unit: " + unit);
        };
        long total = cu.between(
                from.atStartOfDay(MARKET_ZONE),
                to.plusDays(1).atStartOfDay(MARKET_ZONE));
        return (total <= 0) ? 0 : (long) Math.ceil((double) total / interval);
    }
}
