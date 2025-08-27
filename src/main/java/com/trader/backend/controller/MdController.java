package com.trader.backend.controller;

import com.trader.backend.service.CandleService;
import com.trader.backend.service.CandleService.CandleResponse;
import com.trader.backend.service.LiveFeedService;
import com.trader.backend.service.NseInstrumentService;
import com.trader.backend.service.SelectionService;
import com.trader.backend.events.LtpEvent;
import com.trader.backend.service.MarketHours;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/md")
@RequiredArgsConstructor
@Slf4j
public class MdController {
    private final CandleService candleService;
    private final LiveFeedService liveFeedService;
    private final NseInstrumentService nseInstrumentService;
    private final SelectionService selectionService;

    @GetMapping("/candles")
    public Mono<List<CandleResponse>> candles(@RequestParam("instrumentKey") List<String> instrumentKeys,
                                             @RequestParam(value = "tf", defaultValue = "1m") String tf,
                                             @RequestParam(value = "lookback", defaultValue = "120") int lookback) {
        if (instrumentKeys == null || instrumentKeys.isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "instrumentKey required");
        }
        if (!Set.of("1m","3m","5m","15m").contains(tf)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "invalid tf");
        }
        int lb = Math.min(Math.max(lookback,1), 720);
        return candleService.fetchCandles(instrumentKeys, tf, lb);
    }

    @GetMapping("/selection")
    public ResponseEntity<Map<String, Object>> selection() {
        var sel = nseInstrumentService.currentSelectionData();
        String main = nseInstrumentService.nearestNiftyFutureKey().orElse("");
        List<String> opts = sel.keys().stream()
                .filter(k -> !k.equals(main))
                .toList();
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("mainInstrument", main);
        payload.put("options", opts);
        return ResponseEntity.ok(payload);
    }

    @GetMapping("/last-ltp")
    public ResponseEntity<Map<String, Object>> lastLtp(@RequestParam("instrumentKey") String instrumentKey) {
        return liveFeedService.lastQuote(instrumentKey)
                .map(q -> {
                    Map<String, Object> body = new LinkedHashMap<>();
                    body.put("instrumentKey", instrumentKey);
                    body.put("ltp", q.ltp());
                    body.put("ts", q.ts().toString());
                    return ResponseEntity.ok(body);
                })
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "no quote"));
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Map<String, Object>>> stream(@RequestParam(value = "instrumentKey", required = false) List<String> keys,
                                                             ServerHttpResponse response) {
        response.getHeaders().set(HttpHeaders.CACHE_CONTROL, "no-cache");
        try {
            boolean open = MarketHours.isOpen(Instant.now());
            Flux<ServerSentEvent<Map<String, Object>>> heartbeat = Flux.interval(Duration.ofSeconds(15))
                    .map(i -> ServerSentEvent.<Map<String, Object>>builder().comment("hb").build());
            if (open) {
                Flux<LtpEvent> flux = liveFeedService.ltpEvents();
                if (keys != null && !keys.isEmpty()) {
                    Set<String> set = new HashSet<>(keys);
                    flux = flux.filter(ev -> set.contains(ev.instrumentKey()));
                }
                Flux<ServerSentEvent<Map<String, Object>>> ticks = flux
                        .map(ev -> {
                            Map<String, Object> data = new LinkedHashMap<>();
                            data.put("instrumentKey", ev.instrumentKey());
                            data.put("ts", ev.timestamp().toString());
                            data.put("ltp", ev.ltp());
                            return ServerSentEvent.<Map<String, Object>>builder(data).event("tick").build();
                        });
                return Flux.merge(ticks, heartbeat);
            } else {
                Map<String, Object> statusData = new LinkedHashMap<>();
                statusData.put("marketClosed", true);
                statusData.put("message", "Market closed — showing last price");
                ServerSentEvent<Map<String, Object>> status = ServerSentEvent.<Map<String, Object>>builder(statusData)
                        .event("status").build();
                List<String> targetKeys;
                if (keys != null && !keys.isEmpty()) {
                    targetKeys = keys;
                } else {
                    targetKeys = new ArrayList<>(liveFeedService.cachedKeys());
                }
                Flux<ServerSentEvent<Map<String, Object>>> tick = Flux.fromIterable(targetKeys)
                        .map(k -> Map.entry(k, liveFeedService.lastQuote(k)))
                        .filter(e -> e.getValue().isPresent())
                        .map(e -> {
                            LiveFeedService.LatestQuote q = e.getValue().get();
                            Map<String, Object> data = new LinkedHashMap<>();
                            data.put("instrumentKey", e.getKey());
                            data.put("ts", q.ts().toString());
                            data.put("ltp", q.ltp());
                            return ServerSentEvent.<Map<String, Object>>builder(data).event("tick").build();
                        });
                return Flux.merge(Flux.concat(Flux.just(status), tick), heartbeat);
            }
        } catch (Exception e) {
            log.error("SSE stream init failed", e);
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("error", "stream-initialization-failed");
            return Flux.just(ServerSentEvent.<Map<String, Object>>builder(err)
                    .event("error").build());
        }
    }

    @GetMapping("/ltp")
    public ResponseEntity<Map<String, Object>> getNiftyFutLtp() {
        String instKey = selectionService.getMainFutureKey();
        LiveFeedService.LatestQuote quote = liveFeedService.lastQuote(instKey).orElse(null);
        Double ltp = quote != null ? quote.ltp() : null;
        Instant ts = quote != null ? quote.ts() : null;
        if (ltp == null) {
            ltp = candleService.readLatestLtpFromInflux(instKey);
            if (ltp != null) {
                ts = Instant.now();
            }
        }
        if (ltp == null || ts == null) {
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("error", "ltp-unavailable");
            return ResponseEntity.status(503).body(err);
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("instrumentKey", instKey);
        body.put("ltp", ltp);
        body.put("timestamp", ts.toString());
        return ResponseEntity.ok(body);
    }
}
