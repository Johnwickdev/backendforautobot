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

    public List<Candle> getOrFetchHistory(String instrumentKey,
                                          String unit,
                                          int interval,
                                          LocalDate from,
                                          LocalDate to) {
        String normalizedUnit = normalizeUnit(unit);
        LocalDate start = Objects.requireNonNull(from, "from");
        LocalDate end   = Objects.requireNonNull(to,   "to");

        // 1) read cache
        Instant fromInstant = start.atStartOfDay(MARKET_ZONE).toInstant();
        Instant toInstant   = end.plusDays(1).atStartOfDay(MARKET_ZONE).minusNanos(1).toInstant();
        List<Candle> cached = candleRepository
                .findByInstrumentKeyAndUnitAndIntervalAndTsBetween(instrumentKey, normalizedUnit, interval, fromInstant, toInstant);

        // 2) ensure token; if not, just return cached
        if (!ensureToken()) return sortByTimestamp(cached);
        String accessToken = upstoxAuthService.getAccessToken();
        if (accessToken == null || accessToken.isBlank()) return sortByTimestamp(cached);

        // 3) iterate in API-friendly chunks and fetch what might be missing
        List<Candle> fetchedAll = new ArrayList<>();
        LocalDate cursorTo = end;

        while (!cursorTo.isBefore(start)) {
            LocalDate chunkFrom;
            if ("minute".equals(normalizedUnit)) {
                chunkFrom = cursorTo.minusDays(CHUNK_DAYS_MINUTE - 1L);
            } else {
                chunkFrom = cursorTo.minusDays(CHUNK_DAYS_DAY - 1L);
            }
            if (chunkFrom.isBefore(start)) chunkFrom = start;

            List<Candle> part = callUpstox(instrumentKey, normalizedUnit, interval, chunkFrom, cursorTo, accessToken);
            if (!part.isEmpty()) {
                candleRepository.saveAll(part);
                fetchedAll.addAll(part);
            }

            // move back one day to avoid overlap
            cursorTo = chunkFrom.minusDays(1);
        }

        // 4) merge (prefer freshest duplicate by ts)
        Map<String, Candle> map = new HashMap<>();
        for (Candle c : cached) map.put(c.getId(), c);
        for (Candle c : fetchedAll) map.put(c.getId(), c);

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

    private List<Candle> fetchFromUpstox(String instrumentKey,
                                          String unit,
                                          int interval,
                                          LocalDate from,
                                          LocalDate to,
                                          String accessToken) {
        try {
            UriComponentsBuilder builder = UriComponentsBuilder
                    .fromHttpUrl("https://api.upstox.com/v3/historical-candle")
                    .pathSegment(instrumentKey)
                    .pathSegment(unit)
                    .pathSegment(String.valueOf(interval))
                    .queryParam("to_date", DATE_FORMATTER.format(to));
            if (from != null) {
                builder.queryParam("from_date", DATE_FORMATTER.format(from));
            }
            URI uri = builder.build()
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

            if (response == null) {
                log.warn("No response received from Upstox historical candles API");
                return List.of();
            }

            JsonNode candlesNode = response.path("data").path("candles");
            if (!candlesNode.isArray()) {
                log.warn("Unexpected historical candle payload: {}", response);
                return List.of();
            }

            List<Candle> candles = new ArrayList<>();
            for (JsonNode row : candlesNode) {
                if (!row.isArray() || row.size() < 6) {
                    continue;
                }
                Instant ts = parseTimestamp(row.get(0).asText(null));
                if (ts == null) {
                    continue;
                }
                double open = row.get(1).asDouble();
                double high = row.get(2).asDouble();
                double low = row.get(3).asDouble();
                double close = row.get(4).asDouble();
                long volume = row.get(5).isNumber() ? row.get(5).longValue() : 0L;
                long openInterest = row.size() > 6 && row.get(6).isNumber() ? row.get(6).longValue() : 0L;
                candles.add(Candle.create(instrumentKey, unit, interval, ts, open, high, low, close, volume, openInterest));
            }
            return candles;
        } catch (Exception e) {
            log.warn("Failed to fetch historical candles from Upstox", e);
            return List.of();
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
