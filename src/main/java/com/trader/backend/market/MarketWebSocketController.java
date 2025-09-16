package com.trader.backend.market;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trader.backend.service.LiveFeedService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.CollectionUtils;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.CloseStatus;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class MarketWebSocketController {

    private final LiveFeedService liveFeedService;
    private final TickNormalizer tickNormalizer;
    private final ObjectMapper objectMapper;

    @Bean
    public HandlerMapping marketWebSocketMapping(WebSocketHandler marketWebSocketHandler) {
        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setOrder(-1);
        mapping.setUrlMap(Map.of("/ws/market", marketWebSocketHandler));
        return mapping;
    }

    @Bean
    public WebSocketHandler marketWebSocketHandler() {
        return new MarketSocketHandler(liveFeedService, tickNormalizer, objectMapper);
    }

    @Bean
    public WebSocketHandlerAdapter marketWebSocketHandlerAdapter() {
        return new WebSocketHandlerAdapter();
    }

    static final class MarketSocketHandler implements WebSocketHandler {

        private final LiveFeedService liveFeedService;
        private final TickNormalizer tickNormalizer;
        private final ObjectMapper objectMapper;

        MarketSocketHandler(LiveFeedService liveFeedService,
                            TickNormalizer tickNormalizer,
                            ObjectMapper objectMapper) {
            this.liveFeedService = liveFeedService;
            this.tickNormalizer = tickNormalizer;
            this.objectMapper = objectMapper;
        }

        @Override
        public Mono<Void> handle(WebSocketSession session) {
            Set<String> keys = extractKeys(session.getHandshakeInfo().getUri());
            if (CollectionUtils.isEmpty(keys)) {
                log.warn("event=CLIENT_WS_REJECTED instrumentKey=NONE reason=missing_keys");
                return session.close(CloseStatus.PROTOCOL_ERROR);
            }

            String joinedKeys = String.join(",", keys);
            log.info("event=CLIENT_WS_CONNECTED instrumentKey={}", joinedKeys);

            Flux<MarketDtos.NormalizedTick> normalizedFlux = liveFeedService.stream()
                    .flatMap(tick -> filterAndNormalize(tick, keys))
                    .doOnNext(t -> log.info("event=TICK_EMITTED instrumentKey={}", t.instrumentKey()));

            Flux<WebSocketMessage> outbound = normalizedFlux
                    .map(tick -> toMessage(session, tick))
                    .filter(Objects::nonNull);

            Mono<Void> send = session.send(outbound)
                    .doOnError(err -> log.error("event=CLIENT_WS_SEND_FAILED instrumentKey={} reason={}", joinedKeys, err.getMessage()));

            Mono<Void> receive = session.receive()
                    .doOnError(err -> log.error("event=CLIENT_WS_RECEIVE_FAILED instrumentKey={} reason={}", joinedKeys, err.getMessage()))
                    .then();

            return Mono.when(send, receive)
                    .doFinally(signalType -> log.info("event=CLIENT_WS_DISCONNECTED instrumentKey={}", joinedKeys));
        }

        private Flux<MarketDtos.NormalizedTick> filterAndNormalize(JsonNode tick, Set<String> keys) {
            JsonNode feeds = tick.path("feeds");
            if (feeds.isMissingNode() || feeds.isNull()) {
                return Flux.empty();
            }
            List<MarketDtos.NormalizedTick> results = new ArrayList<>();
            for (String key : keys) {
                JsonNode feed = feeds.path(key);
                if (feed.isMissingNode() || feed.isNull()) {
                    continue;
                }
                log.info("event=UPSTREAM_TICK_RCVD instrumentKey={}", key);
                tickNormalizer.normalize(key, feed, tick).ifPresent(results::add);
            }
            return Flux.fromIterable(results);
        }

        private WebSocketMessage toMessage(WebSocketSession session, MarketDtos.NormalizedTick tick) {
            try {
                String json = objectMapper.writeValueAsString(tick);
                return session.textMessage(json);
            } catch (JsonProcessingException e) {
                log.error("event=TICK_SERIALIZATION_FAILED instrumentKey={} reason={}", tick.instrumentKey(), e.getMessage());
                return null;
            }
        }

        private Set<String> extractKeys(URI uri) {
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
}
