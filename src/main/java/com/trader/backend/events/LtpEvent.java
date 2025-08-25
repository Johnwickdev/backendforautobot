package com.trader.backend.events;

import java.time.Instant;

/**
 * Lightweight LTP event published internally for potential UI consumers.
 */
public record LtpEvent(String instrumentKey, double ltp, Instant timestamp) {}
