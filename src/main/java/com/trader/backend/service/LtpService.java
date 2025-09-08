package com.trader.backend.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class LtpService {
    private final LiveFeedService liveFeedService;

    public record Result(Double ltp, Instant ts, String source) {}

    public Result resolve(String instrumentKey) {
        Instant now = Instant.now();
        boolean marketOpen = liveFeedService.isMarketOpen();
        Optional<Tick> live = liveFeedService.getLatestTick(instrumentKey)
                .filter(t -> marketOpen ? Duration.between(t.ts(), now).toMillis() <= 5000 : true);
        if (live.isPresent()) {
            Tick t = live.get();
            liveFeedService.logResolvedLtp(instrumentKey, t.ltp(), "live");
            return new Result(t.ltp(), t.ts(), "live");
        }
        return new Result(null, null, "none");
    }
}
