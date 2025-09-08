package com.trader.backend.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Resolves the latest traded price for any instrument using the live
 * WebSocket feed. If the feed is disconnected or stale, the last
 * tick seen on the WebSocket is returned instead.
 */

@Service
@RequiredArgsConstructor
@Slf4j
public class LtpService {
    private final LiveFeedService liveFeedService;

    public record Result(Double ltp, Instant ts, String source) {}

    public Result resolve(String instrumentKey) {
        Instant now = Instant.now();
        Optional<Tick> tickOpt = liveFeedService.getLatestTick(instrumentKey);
        if (tickOpt.isEmpty()) {
            return new Result(null, null, "none");
        }

        Tick t = tickOpt.get();
        boolean connected = liveFeedService.isConnected();
        boolean marketOpen = liveFeedService.isMarketOpen();
        boolean recent = Duration.between(t.ts(), now).toMillis() <= 5000;

        String source = (connected && marketOpen && recent) ? "live" : "ws-cache";
        liveFeedService.logResolvedLtp(instrumentKey, t.ltp(), source);
        if ("ws-cache".equals(source)) {
            log.info("ltp for nifty future = {}", t.ltp());
        }
        return new Result(t.ltp(), t.ts(), source);
    }
}
