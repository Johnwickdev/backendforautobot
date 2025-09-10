package com.trader.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.upstox.ApiClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import java.util.Base64;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Handles OAuth with Upstox and keeps the current access/refresh tokens
 * in-memory.  It exposes a small event bus so other services can react to
 * authentication changes without crashing the application when tokens are
 * missing or expired.
 */
@Service
@Slf4j
public class UpstoxAuthService {

    private final WebClient webClient = WebClient.create("https://api.upstox.com/v2");
    private final ObjectMapper mapper = new ObjectMapper();
    private final ApiClient apiClient;
    private final LiveFeedService liveFeed;

    @Value("${upstox.apiKey:}")
    private String apiKey;

    @Value("${upstox.apiSecret:}")
    private String apiSecret;

    @Value("${upstox.webhookUri:}")
    private String webhookUri;

    private final AtomicReference<String> accessToken = new AtomicReference<>();
    private final AtomicReference<String> refreshToken = new AtomicReference<>();
    private final AtomicReference<Long> expiresAt = new AtomicReference<>(0L); // epoch seconds

    private final Path tokenFile = Paths.get("token.json");

    private final Sinks.Many<AuthEvent> authEvents = Sinks.many().replay().latest();

    public UpstoxAuthService(ApiClient apiClient, @Lazy LiveFeedService liveFeed) {
        this.apiClient = apiClient;
        this.liveFeed = liveFeed;
    }

    /**
     * Log a friendly message at boot so Railway users know the backend is
     * waiting for a login.  This method never fails.
     */
    @PostConstruct
    void init() {
        log.info("No Upstox token yet — waiting for OAuth login (GET /auth/url).");
        authEvents.tryEmitNext(AuthEvent.WAITING);
    }

    /** Event stream for authentication state changes. */
    public Flux<AuthEvent> events() {
        return authEvents.asFlux();
    }

    /**
     * Exchange the authorization code for tokens and store them in-memory.
     * Emits {@link AuthEvent#READY} on success.
     */
    public Mono<Void> exchangeCode(String code) {
        if (apiKey.isBlank() || apiSecret.isBlank() || webhookUri.isBlank()) {
            return Mono.error(new IllegalStateException("Upstox credentials not configured"));
        }

        return webClient.post()
                .uri("/login/authorization/token")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .body(BodyInserters.fromFormData("code", code)
                        .with("client_id", apiKey)
                        .with("client_secret", apiSecret)
                        .with("redirect_uri", webhookUri)
                        .with("grant_type", "authorization_code"))
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                .doOnNext(tok -> {
                    String at = (String) tok.get("access_token");
                    if (at == null || at.isBlank()) {
                        log.warn("Invalid token response: {}", tok);
                        return;
                    }

                    accessToken.set(at);

                    String rt = (String) tok.get("refresh_token");
                    if (rt != null && !rt.isBlank()) {
                        refreshToken.set(rt);
                    }

                    Number ttlNum = (Number) tok.get("expires_in");
                    long exp;
                    if (ttlNum != null) {
                        exp = System.currentTimeMillis() / 1000 + ttlNum.longValue();
                    } else {
                        exp = extractExpiry(at);
                    }
                    expiresAt.set(exp);
                    apiClient.addDefaultHeader("Authorization", "Bearer " + at);
                    saveTokenBundle(at, refreshToken.get(), exp);
                    authEvents.tryEmitNext(AuthEvent.READY);
                    log.info("✅ access_token saved (expires {})", exp > 0 ? Instant.ofEpochSecond(exp) : "unknown");
                })
                .then();
    }

    /**
     * Ensure the current token is still valid.  Never throws.  If the token is
     * missing or a refresh fails, {@code Mono.just(false)} is returned.
     */
    public Mono<Boolean> ensureValidToken() {
        long now = System.currentTimeMillis() / 1000;
        String at = accessToken.get();
        if (at == null) {
            log.warn("No Upstox token available; awaiting login");
            authEvents.tryEmitNext(AuthEvent.WAITING);
            return Mono.just(false);
        }

        long exp = expiresAt.get();
        long remaining = exp - now;
        if (exp == 0 || remaining <= 60) {
            log.warn("Access token expiring soon; attempting refresh");
            return refreshToken()
                    .onErrorResume(e -> {
                        log.warn("Token refresh failed", e);
                        authEvents.tryEmitNext(AuthEvent.EXPIRED);
                        return Mono.just(false);
                    });
        }

        return Mono.just(true);
    }

    /**
     * Refresh the current access token.  Emits {@link AuthEvent#READY} on
     * success and {@link AuthEvent#EXPIRED} on failure.
     */
    private Mono<Boolean> refreshToken() {
        String rt = refreshToken.get();
        if (rt == null) {
            authEvents.tryEmitNext(AuthEvent.EXPIRED);
            return Mono.just(false);
        }

        return webClient.post()
                .uri("/login/authorization/token")
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .body(BodyInserters.fromFormData("grant_type", "refresh_token")
                        .with("refresh_token", rt)
                        .with("client_id", apiKey)
                        .with("client_secret", apiSecret))
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                .map(tok -> {
                    String at = (String) tok.get("access_token");
                    Integer ttl = (Integer) tok.get("expires_in");
                    String newRt = (String) tok.get("refresh_token");
                    if (at == null || ttl == null) {
                        return false;
                    }
                    accessToken.set(at);
                    if (newRt != null) refreshToken.set(newRt);
                    long exp = System.currentTimeMillis() / 1000 + ttl;
                    expiresAt.set(exp);
                    apiClient.addDefaultHeader("Authorization", "Bearer " + at);
                    saveTokenBundle(at, refreshToken.get(), exp);
                    log.info("🔄 Refreshed access_token ({} sec left)", ttl);
                    authEvents.tryEmitNext(AuthEvent.READY);
                    return true;
                })
                .onErrorResume(e -> {
                    log.warn("Refresh call failed", e);
                    authEvents.tryEmitNext(AuthEvent.EXPIRED);
                    return Mono.just(false);
                });
    }

    /** Force a refresh regardless of expiry. */
    public Mono<Boolean> forceRefreshToken() {
        return refreshToken();
    }

    /**
     * Parse the JWT payload to extract the exp claim when expires_in is missing.
     */
    private long extractExpiry(String jwt) {
        try {
            String[] parts = jwt.split("\\.");
            if (parts.length < 2) return 0L;
            String payload = new String(Base64.getUrlDecoder().decode(parts[1]));
            JsonNode node = mapper.readTree(payload);
            return node.path("exp").asLong(0L);
        } catch (Exception e) {
            log.warn("Failed to parse JWT exp", e);
            return 0L;
        }
    }

    /** Current token for other services. */
    public String currentToken() {
        return accessToken.get();
    }

    public void setCurrentToken(String token) {
        accessToken.set(token);
        apiClient.addDefaultHeader("Authorization", "Bearer " + token);
    }

    /**
     * Simple status endpoint helper.
     */
    public Mono<Map<String, Object>> status() {
        TokenBundle bundle = loadTokenBundle();
        long now = System.currentTimeMillis() / 1000;
        long exp = bundle != null ? bundle.expiresAt : 0;
        boolean connected = bundle != null && bundle.accessToken != null && now < exp;

        Map<String, Object> m = new HashMap<>();
        m.put("connected", connected);
        m.put("expiresAt", connected ? Instant.ofEpochSecond(exp).toString() : null);
        m.put("remainingSeconds", connected ? (int) (exp - now) : 0);
        return Mono.just(m);
    }

    private void saveTokenBundle(String at, String rt, long exp) {
        try {
            TokenBundle b = new TokenBundle(at, rt, exp);
            mapper.writeValue(tokenFile.toFile(), b);
        } catch (IOException e) {
            log.warn("Failed to persist token bundle", e);
        }
    }

    private TokenBundle loadTokenBundle() {
        try {
            if (Files.exists(tokenFile)) {
                return mapper.readValue(tokenFile.toFile(), TokenBundle.class);
            }
        } catch (IOException e) {
            log.warn("Failed to read token bundle", e);
        }
        return null;
    }

    /** Convenience method used by controllers to fetch market quotes. */
    public Mono<Map<String, Object>> fetchQuote(String exchange, String symbol) {
        String token = accessToken.get();
        if (token == null) {
            log.warn("Access-token not ready. Hit /auth/exchange first.");
            return Mono.just(new LinkedHashMap<>());
        }

        return webClient.get()
                .uri(uriBuilder -> uriBuilder
                        .path("/market-quote/quotes")
                        .queryParam("symbols", exchange + ":" + symbol)
                        .build())
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .header(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON_VALUE)
                .retrieve()
                .bodyToMono(new ParameterizedTypeReference<Map<String, Object>>() {})
                .map(resp -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> data = (Map<String, Object>) resp.get("data");
                    @SuppressWarnings("unchecked")
                    Map<String, Object> quote = (Map<String, Object>) data.get(exchange + ":" + symbol);
                    return quote;
                });
    }

    /** For webhook based auth flows (unused but kept for completeness). */
    public Mono<Void> handleWebhook(String rawJson) {
        String code;
        try {
            JsonNode node = mapper.readTree(rawJson);
            code = node.path("code").asText(null);
            if (code == null || code.isBlank()) {
                log.warn("Webhook hit but no ?code= param found -> {}", rawJson);
                return Mono.empty();
            }
        } catch (IOException e) {
            return Mono.error(e);
        }

        return exchangeCode(code);
    }

    /** Build the Upstox OAuth dialog URL for the frontend. */
    public String buildAuthUrl() {
        if (apiKey.isBlank() || webhookUri.isBlank()) {
            throw new IllegalStateException("Upstox credentials not configured");
        }

        return UriComponentsBuilder
                .fromUriString("https://api.upstox.com/v2/login/authorization/dialog")
                .queryParam("response_type", "code")
                .queryParam("client_id", apiKey)
                .queryParam("redirect_uri", webhookUri)
                .queryParam("state", "botInit")
                .queryParam("scope", "profile marketdata")
                .build().toUriString();
    }

    /** Authentication lifecycle events. */
    public enum AuthEvent { WAITING, READY, EXPIRED }
}

