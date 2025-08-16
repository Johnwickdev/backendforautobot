package com.trader.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple quantitative analysis on incoming tick data. The goal is to
 * compute a few microstructure features (momentum, volume spike and
 * order book imbalance) and produce coarse entry/exit signals. This is
 * intentionally lightweight and stateful per instrument key so it can
 * evolve over time as more ticks arrive.
 */
@Service
@Slf4j
public class QuantAnalysisService {

    private static final int WINDOW = 20;
    private static final double MOMENTUM_THRESHOLD = 0.001; // 0.1%
    private static final double VOLUME_SPIKE_FACTOR = 1.5;
    private static final double IMBALANCE_THRESHOLD = 0.2;
    private static final double NOISE_THRESHOLD = 0.6; // fraction of direction flips

    private final Map<String, Deque<Double>> midHistory = new ConcurrentHashMap<>();
    private final Map<String, Deque<Double>> volHistory = new ConcurrentHashMap<>();
    private final Map<String, Deque<Integer>> dirHistory = new ConcurrentHashMap<>();

    public QuantAnalysisResult analyze(String instrumentKey, JsonNode feed) {
        JsonNode marketFF = feed.path("fullFeed").path("marketFF");

        double mid = computeMidPrice(marketFF);
        Deque<Double> mids = midHistory.computeIfAbsent(instrumentKey, k -> new ArrayDeque<>());
        double prevMid = mids.isEmpty() ? mid : mids.getLast();
        double momentum = computeMomentum(prevMid, mid);
        double noise = computeNoise(instrumentKey, prevMid, mid);
        mids.addLast(mid);
        if (mids.size() > WINDOW) mids.removeFirst();
        double ltq = marketFF.path("ltpc").path("ltq").asDouble(0);
        double volSpike = computeVolumeSpike(instrumentKey, ltq);
        double imbalance = computeOrderBookImbalance(marketFF.path("marketLevel").path("bidAskQuote"));

        Signal signal = determineSignal(momentum, volSpike, imbalance, noise);
        return new QuantAnalysisResult(momentum, volSpike, imbalance, noise, signal);
    }

    private double computeMidPrice(JsonNode marketFF) {
        JsonNode quotes = marketFF.path("marketLevel").path("bidAskQuote");
        if (!quotes.isArray() || quotes.size() == 0) {
            return marketFF.path("ltpc").path("ltp").asDouble(0);
        }
        JsonNode top = quotes.get(0);
        double bid = top.path("bidP").asDouble(0);
        double ask = top.path("askP").asDouble(0);
        return (bid + ask) / 2.0;
    }

    private double computeOrderBookImbalance(JsonNode quotes) {
        if (!quotes.isArray() || quotes.size() == 0) return 0;
        double bidSum = 0;
        double askSum = 0;
        for (JsonNode level : quotes) {
            bidSum += level.path("bidQ").asDouble(0);
            askSum += level.path("askQ").asDouble(0);
        }
        double denom = bidSum + askSum;
        if (denom == 0) return 0;
        return (bidSum - askSum) / denom;
    }

    private double computeMomentum(double prev, double mid) {
        if (prev == 0) return 0;
        return (mid - prev) / prev;
    }

    private double computeVolumeSpike(String key, double ltq) {
        Deque<Double> deque = volHistory.computeIfAbsent(key, k -> new ArrayDeque<>());
        double avg = deque.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        deque.addLast(ltq);
        if (deque.size() > WINDOW) deque.removeFirst();
        if (avg == 0) return 0;
        return ltq / avg;
    }

    private Signal determineSignal(double momentum, double volSpike, double imbalance, double noise) {
        if (noise > NOISE_THRESHOLD) {
            return Signal.EXIT;
        }
        if (momentum > MOMENTUM_THRESHOLD && volSpike > VOLUME_SPIKE_FACTOR && imbalance > IMBALANCE_THRESHOLD) {
            return Signal.ENTRY_LONG;
        }
        if (momentum < -MOMENTUM_THRESHOLD && volSpike > VOLUME_SPIKE_FACTOR && imbalance < -IMBALANCE_THRESHOLD) {
            return Signal.ENTRY_SHORT;
        }
        if (Math.abs(momentum) < MOMENTUM_THRESHOLD || Math.abs(imbalance) < IMBALANCE_THRESHOLD) {
            return Signal.EXIT;
        }
        return Signal.NONE;
    }

    private double computeNoise(String key, double prev, double mid) {
        Deque<Integer> deque = dirHistory.computeIfAbsent(key, k -> new ArrayDeque<>());
        int dir = 0;
        if (mid > prev) dir = 1; else if (mid < prev) dir = -1; else dir = 0;
        if (dir != 0) {
            deque.addLast(dir);
            if (deque.size() > WINDOW) deque.removeFirst();
        }
        if (deque.size() < 2) return 0;
        int flips = 0;
        Integer[] arr = deque.toArray(new Integer[0]);
        for (int i = 1; i < arr.length; i++) {
            if (!arr[i].equals(arr[i-1])) flips++;
        }
        return (double) flips / (arr.length - 1);
    }

    public record QuantAnalysisResult(double momentum, double volumeSpike, double imbalance, double noise, Signal signal) { }

    public enum Signal { ENTRY_LONG, ENTRY_SHORT, EXIT, NONE }
}

