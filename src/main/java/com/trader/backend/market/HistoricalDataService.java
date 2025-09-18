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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

@Service
@RequiredArgsConstructor
@Slf4j
public class HistoricalDataService {

    private static final ZoneId MARKET_ZONE = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter LEGACY_TS_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm").withZone(MARKET_ZONE);
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
        LocalDate sanitizedFrom = Objects.requireNonNull(from, "from");
        LocalDate sanitizedTo = Objects.requireNonNull(to, "to");

        Instant fromInstant = sanitizedFrom.atStartOfDay(MARKET_ZONE).toInstant();
        Instant toInstant = sanitizedTo.plusDays(1).atStartOfDay(MARKET_ZONE).minusNanos(1).toInstant();

        List<Candle> cached = new ArrayList<>(candleRepository
                .findByInstrumentKeyAndUnitAndIntervalAndTsBetween(instrumentKey, normalizedUnit, interval, fromInstant, toInstant));
        for (String alias : legacyAliases(normalizedUnit)) {
            cached.addAll(candleRepository
                    .findByInstrumentKeyAndUnitAndIntervalAndTsBetween(instrumentKey, alias, interval, fromInstant, toInstant));
        }
        List<Candle> existing = dedupe(cached);

        long expected = expectedCount(normalizedUnit, interval, sanitizedFrom, sanitizedTo);
        if (expected <= 0) {
            log.debug("Expected count for {} {} {} from {} to {} is <= 0; returning cached {} candles",
                    instrumentKey, normalizedUnit, interval, sanitizedFrom, sanitizedTo, existing.size());
            return sortByTimestamp(existing);
        }

        if (existing.size() >= expected) {
            return sortByTimestamp(existing);
        }

        if (!ensureToken()) {
            log.warn("Unable to validate Upstox token; returning cached candles ({} entries)", existing.size());
            return sortByTimestamp(existing);
        }

        String accessToken = upstoxAuthService.getAccessToken();
        if (accessToken == null || accessToken.isBlank()) {
            log.warn("No Upstox access token available; returning cached candles ({} entries)", existing.size());
            return sortByTimestamp(existing);
        }

        List<Candle> fetched = fetchFromUpstox(instrumentKey, normalizedUnit, interval, sanitizedFrom, sanitizedTo, accessToken);
        if (CollectionUtils.isEmpty(fetched)) {
            return sortByTimestamp(existing);
        }

        candleRepository.saveAll(fetched);

        Map<String, Candle> combined = new HashMap<>();
        for (Candle candle : existing) {
            combined.put(candle.getId(), candle);
        }
        for (Candle candle : fetched) {
            combined.put(candle.getId(), candle);
        }
        return sortByTimestamp(new ArrayList<>(combined.values()));
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

    private List<String> legacyAliases(String unit) {
        return switch (unit) {
            case "minute" -> List.of("minutes");
            case "hour" -> List.of("hours");
            case "day" -> List.of("days");
            case "week" -> List.of("weeks");
            case "month" -> List.of("months");
            default -> List.of();
        };
    }

    private List<Candle> dedupe(List<Candle> candles) {
        Map<String, Candle> unique = new LinkedHashMap<>();
        List<Candle> withoutId = new ArrayList<>();
        for (Candle candle : candles) {
            if (candle == null) {
                continue;
            }
            String id = candle.getId();
            if ((id == null || id.isBlank()) && candle.getTs() != null) {
                id = candle.getInstrumentKey() + "/" + candle.getTs().toEpochMilli();
            }
            if (id == null || id.isBlank()) {
                withoutId.add(candle);
                continue;
            }
            unique.put(id, candle);
        }
        List<Candle> result = new ArrayList<>(unique.values());
        result.addAll(withoutId);
        return result;
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
            String path = UriComponentsBuilder
                    .fromPath("/historical-candle/{instrumentKey}/{unit}/{interval}")
                    .queryParam("to_date", DATE_FORMATTER.format(to))
                    .queryParam("from_date", DATE_FORMATTER.format(from))
                    .buildAndExpand(instrumentKey, unit, interval)
                    .encode()
                    .toUriString();

            WebClient client = webClientBuilder.clone()
                    .baseUrl("https://api.upstox.com/v3")
                    .build();
            JsonNode response = client.get()
                    .uri(path)
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
        ChronoUnit chronoUnit;
        switch (unit) {
            case "minute" -> chronoUnit = ChronoUnit.MINUTES;
            case "hour" -> chronoUnit = ChronoUnit.HOURS;
            case "day" -> chronoUnit = ChronoUnit.DAYS;
            case "week" -> chronoUnit = ChronoUnit.WEEKS;
            case "month" -> chronoUnit = ChronoUnit.MONTHS;
            default -> throw new IllegalArgumentException("Unsupported unit: " + unit);
        }
        long totalUnits = chronoUnit.between(from.atStartOfDay(MARKET_ZONE), to.plusDays(1).atStartOfDay(MARKET_ZONE));
        if (totalUnits <= 0) {
            return 0;
        }
        return (long) Math.ceil((double) totalUnits / interval);
    }
}
