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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Service
@RequiredArgsConstructor
@Slf4j
public class HistoricalDataService {
    private static final int CHUNK_DAYS_MINUTE = 60;   // safe window for minute candles
    private static final int CHUNK_DAYS_DAY    = 365;  // yearly chunks for day candles

    private static final ZoneId MARKET_ZONE = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter LEGACY_TS_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").withZone(MARKET_ZONE);
    private static final Set<String> SUPPORTED_UNITS =
            Set.of("minute", "minutes", "hour", "hours", "day", "days", "week", "weeks", "month", "months");
    private static final Duration API_TIMEOUT = Duration.ofSeconds(15);

    private final WebClient.Builder webClientBuilder;
    private final UpstoxAuthService upstoxAuthService;
    private final CandleRepository candleRepository;

    // REPLACE your fetchFromUpstox + callUpstox with this single helper:

    private List<Candle> fetchWindowFromUpstox(
            String instrumentKey,
            String unit,          // "minute" | "day"
            int interval,
            LocalDate from,       // inclusive
            LocalDate to,         // inclusive
            String accessToken) {

        try {
            // If minute candles and the window touches today, use the intraday endpoint for [today].
            boolean touchesToday = !to.isBefore(LocalDate.now(MARKET_ZONE));
            boolean isMinute = "minute".equals(unit);

            List<Candle> out = new ArrayList<>();
            WebClient client = webClientBuilder.clone().build();

            if (isMinute && touchesToday && !from.isAfter(LocalDate.now(MARKET_ZONE))) {
                // 1) Intraday for today
                URI intradayUri = UriComponentsBuilder
                        .fromHttpUrl("https://api.upstox.com/v3/historical-candle")
                        .pathSegment("intraday")
                        .pathSegment(instrumentKey)
                        .pathSegment(unit)
                        .pathSegment(String.valueOf(interval))
                        .build()
                        .encode()
                        .toUri();

                JsonNode intraday = client.get()
                        .uri(intradayUri)
                        .header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                        .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                        .retrieve()
                        .bodyToMono(JsonNode.class)
                        .block(API_TIMEOUT);

                parseCandlesJson(instrumentKey, unit, interval, intraday, out);
            }

            // 2) Historical (with dates in PATH, newest-first chunk)
            // Upstox V3 paths:
            // /v3/historical-candle/{instrumentKey}/{unit}/{interval}/{to_date}
            // /v3/historical-candle/{instrumentKey}/{unit}/{interval}/{to_date}/{from_date}
            URI histUri = UriComponentsBuilder
                    .fromHttpUrl("https://api.upstox.com/v3/historical-candle")
                    .pathSegment(instrumentKey)
                    .pathSegment(unit)
                    .pathSegment(String.valueOf(interval))
                    .pathSegment(DATE_FORMATTER.format(to))
                    .pathSegment(DATE_FORMATTER.format(from))
                    .build()
                    .encode()
                    .toUri();

            JsonNode hist = client.get()
                    .uri(histUri)
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .block(API_TIMEOUT);

            parseCandlesJson(instrumentKey, unit, interval, hist, out);

            out.sort(Comparator.comparing(Candle::getTs));
            return out;

        } catch (Exception e) {
            log.warn("Upstox V3 history fetch failed for {} {} {} {}..{}", instrumentKey, unit, interval, from, to, e);
            return List.of();
        }
    }

    // helper to parse payload into Candle[]
    private void parseCandlesJson(String instrumentKey, String unit, int interval, JsonNode body, List<Candle> sink) {
        if (body == null) return;
        JsonNode rows = body.path("data").path("candles");
        if (!rows.isArray()) return;
        for (JsonNode row : rows) {
            if (!row.isArray() || row.size() < 5) continue;
            Instant ts = parseTimestamp(row.get(0).asText(null));
            if (ts == null) continue;
            double o = row.get(1).asDouble();
            double h = row.get(2).asDouble();
            double l = row.get(3).asDouble();
            double c = row.get(4).asDouble();
            long v = row.size() > 5 && row.get(5).isNumber() ? row.get(5).longValue() : 0L;
            long oi = row.size() > 6 && row.get(6).isNumber() ? row.get(6).longValue() : 0L;
            sink.add(Candle.create(instrumentKey, unit, interval, ts, o, h, l, c, v, oi));
        }
    }

    // UPDATE your getOrFetchHistory to call fetchWindowFromUpstox with chunks using PATH dates
    public List<Candle> getOrFetchHistory(String instrumentKey, String unit, int interval, LocalDate from, LocalDate to) {
        String normalizedUnit = normalizeUnit(unit);
        LocalDate start = Objects.requireNonNull(from, "from");
        LocalDate end   = Objects.requireNonNull(to,   "to");

        // read cache first
        Instant fromInstant = start.atStartOfDay(MARKET_ZONE).toInstant();
        Instant toInstant   = end.plusDays(1).atStartOfDay(MARKET_ZONE).minusNanos(1).toInstant();
        List<Candle> cached = candleRepository
                .findByInstrumentKeyAndUnitAndIntervalAndTsBetween(instrumentKey, normalizedUnit, interval, fromInstant, toInstant);

        if (!ensureToken()) return sortByTimestamp(cached);
        String accessToken = upstoxAuthService.getAccessToken();
        if (accessToken == null || accessToken.isBlank()) return sortByTimestamp(cached);

        // chunk sizes (keep modest for minute)
        final int chunkDays = "minute".equals(normalizedUnit) ? 60 : 365; // minute: ~2 months; day: yearly
        List<Candle> fetched = new ArrayList<>();

        LocalDate cursorTo = end;
        while (!cursorTo.isBefore(start)) {
            LocalDate cursorFrom = cursorTo.minusDays(chunkDays - 1L);
            if (cursorFrom.isBefore(start)) cursorFrom = start;

            List<Candle> part = fetchWindowFromUpstox(
                    instrumentKey, normalizedUnit, interval, cursorFrom, cursorTo, accessToken);

            if (!part.isEmpty()) {
                candleRepository.saveAll(part);
                fetched.addAll(part);
            }

            cursorTo = cursorFrom.minusDays(1);
        }

        // merge
        Map<String, Candle> map = new HashMap<>();
        for (Candle c : cached) map.put(c.getId(), c);
        for (Candle c : fetched) map.put(c.getId(), c);

        return sortByTimestamp(new ArrayList<>(map.values()));
    }

    private List<Candle> callUpstox(String instrumentKey,
                                    String unit,
                                    int interval,
                                    LocalDate from,
                                    LocalDate to,
                                    String accessToken) {
        try {
            URI uri = UriComponentsBuilder
                    .fromHttpUrl("https://api.upstox.com/v3/historical-candle")
                    .pathSegment(instrumentKey)   // safe: encodes '|' -> %7C
                    .pathSegment(unit)
                    .pathSegment(String.valueOf(interval))
                    .queryParam("from_date", DATE_FORMATTER.format(from))
                    .queryParam("to_date",   DATE_FORMATTER.format(to))
                    .build()
                    .encode()
                    .toUri();

            WebClient client = webClientBuilder.clone().build();
            JsonNode response = client.get()
                    .uri(uri)
                    .header(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken)
                    .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .block(API_TIMEOUT);

            if (response == null) return List.of();
            JsonNode candlesNode = response.path("data").path("candles");
            if (!candlesNode.isArray()) return List.of();

            List<Candle> out = new ArrayList<>();
            for (JsonNode row : candlesNode) {
                if (!row.isArray() || row.size() < 5) continue;
                Instant ts = parseTimestamp(row.get(0).asText(null));
                if (ts == null) continue;
                double open  = row.get(1).asDouble();
                double high  = row.get(2).asDouble();
                double low   = row.get(3).asDouble();
                double close = row.get(4).asDouble();
                long volume  = row.size() > 5 && row.get(5).isNumber() ? row.get(5).longValue() : 0L;
                long oi      = row.size() > 6 && row.get(6).isNumber() ? row.get(6).longValue() : 0L;

                out.add(Candle.create(instrumentKey, unit, interval, ts, open, high, low, close, volume, oi));
            }
            return out;
        } catch (Exception ex) {
            log.warn("Upstox history fetch failed for {} {} {} {}..{}",
                    instrumentKey, unit, interval, from, to, ex);
            return List.of();
        }
    }

    private String normalizeUnit(String unit) {
        String normalized = unit == null ? "minute" : unit.toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "minute", "minutes" -> "minute";
            case "hour", "hours" -> "hour";
            case "day", "days" -> "day";
            case "week", "weeks" -> "week";
            case "month", "months" -> "month";
            default -> throw new IllegalArgumentException("Unsupported unit: " + unit);
        };
    }

    private boolean ensureToken() {
        try {
            Mono<Boolean> mono = upstoxAuthService.ensureValidToken();
            Boolean ready = mono == null ? Boolean.FALSE : mono.block(API_TIMEOUT);
            return Boolean.TRUE.equals(ready);
        } catch (Exception e) {
            log.warn("Failed to ensure Upstox token is valid", e);
            return false;
        }
    }



    private Instant parseTimestamp(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException ignored) {
            try {
                LocalDateTime ldt = LocalDateTime.parse(value, LEGACY_TS_FORMATTER);
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

    private long expectedCount(String unit, int interval, LocalDate from, LocalDate to) {
        if (interval <= 0 || to.isBefore(from)) {
            return 0;
        }
        ChronoUnit chronoUnit = switch (unit) {
            case "minute" -> ChronoUnit.MINUTES;
            case "hour" -> ChronoUnit.HOURS;
            case "day" -> ChronoUnit.DAYS;
            case "week" -> ChronoUnit.WEEKS;
            case "month" -> ChronoUnit.MONTHS;
            default -> throw new IllegalArgumentException("Unsupported unit: " + unit);
        };
        long totalUnits = chronoUnit.between(
                from.atStartOfDay(MARKET_ZONE),
                to.plusDays(1).atStartOfDay(MARKET_ZONE));
        if (totalUnits <= 0) {
            return 0;
        }
        return (long) Math.ceil((double) totalUnits / interval);
    }
}
