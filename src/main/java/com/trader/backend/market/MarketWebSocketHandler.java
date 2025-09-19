package com.trader.backend.market;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trader.backend.service.LiveFeedService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MultiValueMap;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Component
@RequiredArgsConstructor
@Slf4j
public class MarketWebSocketHandler extends TextWebSocketHandler {

    private static final String SUBSCRIPTION_ATTRIBUTE = "marketSubscription";

    private final LiveFeedService liveFeedService;
    private final TickNormalizer tickNormalizer;
    private final ObjectMapper objectMapper;

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        Set<String> keys = extractKeys(session.getUri());
        if (CollectionUtils.isEmpty(keys)) {
            log.warn("event=CLIENT_WS_REJECTED instrumentKey=NONE reason=missing_keys");
            session.close(CloseStatus.PROTOCOL_ERROR);
            return;
        }

        String joinedKeys = String.join(",", keys);
        log.info("event=CLIENT_WS_CONNECTED instrumentKey={}", joinedKeys);

        Disposable subscription = liveFeedService.stream()
                .flatMap(tick -> filterAndNormalize(tick, keys))
                .subscribe(
                        tick -> send(session, tick),
                        error -> handleStreamError(session, joinedKeys, error),
                        () -> log.debug("event=CLIENT_WS_COMPLETED instrumentKey={}", joinedKeys)
                );

        session.getAttributes().put(SUBSCRIPTION_ATTRIBUTE, subscription);
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) throws Exception {
        log.error("event=CLIENT_WS_TRANSPORT_ERROR reason={}", exception.getMessage(), exception);
        session.close(CloseStatus.SERVER_ERROR);
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) throws Exception {
        Optional.ofNullable(session.getAttributes().remove(SUBSCRIPTION_ATTRIBUTE))
                .filter(Disposable.class::isInstance)
                .map(Disposable.class::cast)
                .ifPresent(Disposable::dispose);
        log.info("event=CLIENT_WS_DISCONNECTED status={} reason={} code={}",
                status, status.getReason(), status.getCode());
    }

    private void handleStreamError(WebSocketSession session, String keys, Throwable error) {
        log.error("event=CLIENT_WS_STREAM_FAILED instrumentKey={} reason={}", keys, error.getMessage(), error);
        try {
            session.close(CloseStatus.SERVER_ERROR);
        } catch (IOException ioException) {
            log.debug("event=CLIENT_WS_CLOSE_FAILED instrumentKey={} reason={}", keys, ioException.getMessage());
        }
    }

    private void send(WebSocketSession session, MarketDtos.NormalizedTick tick) {
        if (!session.isOpen()) {
            return;
        }
        try {
            String payload = objectMapper.writeValueAsString(tick);
            synchronized (session) {
                if (session.isOpen()) {
                    session.sendMessage(new TextMessage(payload));
                }
            }
        } catch (JsonProcessingException e) {
            log.error("event=TICK_SERIALIZATION_FAILED instrumentKey={} reason={}", tick.instrumentKey(), e.getMessage());
        } catch (IOException e) {
            log.error("event=CLIENT_WS_SEND_FAILED instrumentKey={} reason={}", tick.instrumentKey(), e.getMessage());
        }
    }

    private Flux<MarketDtos.NormalizedTick> filterAndNormalize(JsonNode tick, Set<String> keys) {
        JsonNode feeds = tick.path("feeds");
        if (feeds.isMissingNode() || feeds.isNull()) {
            return Flux.empty();
        }
        return Flux.fromIterable(keys)
                .map(key -> Map.entry(key, feeds.path(key)))
                .filter(entry -> entry.getValue() != null && !entry.getValue().isMissingNode() && !entry.getValue().isNull())
                .map(entry -> tickNormalizer.normalize(entry.getKey(), entry.getValue(), tick))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .doOnNext(normalized -> log.info("event=TICK_EMITTED instrumentKey={}", normalized.instrumentKey()));
    }

    private Set<String> extractKeys(URI uri) {
        if (uri == null) {
            return Set.of();
        }
        MultiValueMap<String, String> queryParams = UriComponentsBuilder.fromUri(uri).build().getQueryParams();
        if (queryParams == null || queryParams.isEmpty()) {
            return Set.of();
        }
        Set<String> keys = new LinkedHashSet<>();
        List<String> rawKeys = queryParams.get("keys");
        if (rawKeys != null) {
            for (String raw : rawKeys) {
                if (raw == null) {
                    continue;
                }
                for (String value : raw.split(",")) {
                    String trimmed = value.trim();
                    if (!trimmed.isEmpty()) {
                        keys.add(trimmed);
                    }
                }
            }
        }
        return keys;
    }
}
