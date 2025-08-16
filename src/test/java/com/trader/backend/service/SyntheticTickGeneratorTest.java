package com.trader.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Basic structural test to ensure the synthetic generator produces
 * ticks resembling the live broker feed.
 */
public class SyntheticTickGeneratorTest {

    @Test
    void generatesTickWithFeeds() {
        SyntheticTickGenerator gen = new SyntheticTickGenerator();
        JsonNode tick = gen.next();
        JsonNode feeds = tick.get("feeds");
        assertNotNull(feeds);
        assertNotNull(feeds.get("NSE_FO|64103"));
        JsonNode fut = feeds.get("NSE_FO|64103");
        assertTrue(fut.path("fullFeed").path("marketFF").path("ltpc").has("ltp"));
    }
}

