package com.trader.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Random;

/**
 * Generates synthetic tick data that mimics the structure of
 * the broker API feed. This allows offline testing without
 * relying on recorded mock samples.
 */
@Component
@Profile("mock")
@Slf4j
public class SyntheticTickGenerator {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Random random = new Random();

    private double futurePrice = 24671.0;
    private double optionPrice = 310.0;
    private long timestamp = System.currentTimeMillis();

    public JsonNode next() {
        timestamp += 250;
        futurePrice = randomWalk(futurePrice, 0.05);
        optionPrice = randomWalk(optionPrice, 0.2);

        ObjectNode root = mapper.createObjectNode();
        ObjectNode feeds = root.putObject("feeds");
        feeds.set("NSE_FO|64103", buildInstrumentNode(futurePrice, 24712.2));
        feeds.set("NSE_FO|47161", buildInstrumentNode(optionPrice, 336.2));
        return root;
    }

    private ObjectNode buildInstrumentNode(double price, double closePrice) {
        ObjectNode instrument = mapper.createObjectNode();
        ObjectNode fullFeed = instrument.putObject("fullFeed");
        ObjectNode marketFF = fullFeed.putObject("marketFF");
        ObjectNode ltpc = marketFF.putObject("ltpc");
        ltpc.put("ltp", price);
        ltpc.put("ltt", String.valueOf(timestamp));
        ltpc.put("ltq", String.valueOf(randomVolume()));
        ltpc.put("cp", closePrice);

        ObjectNode marketLevel = marketFF.putObject("marketLevel");
        ArrayNode quotes = marketLevel.putArray("bidAskQuote");
        double spread = price * 0.0004;
        double bid = price - spread / 2;
        double ask = price + spread / 2;
        quotes.add(buildQuote(bid, ask));
        quotes.add(buildQuote(bid - spread, ask + spread));
        return instrument;
    }

    private ObjectNode buildQuote(double bid, double ask) {
        ObjectNode q = mapper.createObjectNode();
        q.put("bidQ", String.valueOf(randomVolume()));
        q.put("bidP", bid);
        q.put("askQ", String.valueOf(randomVolume()));
        q.put("askP", ask);
        return q;
    }

    private int randomVolume() {
        return 50 + random.nextInt(950);
    }

    private double randomWalk(double price, double volatilityPct) {
        double change = price * volatilityPct / 100.0 * random.nextGaussian();
        return price + change;
    }
}

