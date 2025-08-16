package com.trader.backend.service;

import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

/**
 * Derives a coarse market status (bullish, bearish, neutral) from
 * futures quantitative analysis results. This helps summarize the
 * broader market direction during mock runs.
 */
@Component
@Profile("mock")
public class MarketStatusService {

    private static final double THRESHOLD = 0.0005; // 0.05%

    public MarketStatus determine(QuantAnalysisService.QuantAnalysisResult futuresResult) {
        double m = futuresResult.momentum();
        if (m > THRESHOLD) return MarketStatus.BULLISH;
        if (m < -THRESHOLD) return MarketStatus.BEARISH;
        return MarketStatus.NEUTRAL;
    }

    public enum MarketStatus { BULLISH, BEARISH, NEUTRAL }
}

