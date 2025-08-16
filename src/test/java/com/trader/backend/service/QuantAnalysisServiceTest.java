package com.trader.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class QuantAnalysisServiceTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void detectsEntrySignal() throws Exception {
        String tick1 = "{" +
                "\"fullFeed\": { \"marketFF\": {" +
                "\"ltpc\": {\"ltp\":310.0,\"ltq\":\"75\"}," +
                "\"marketLevel\": { \"bidAskQuote\": [" +
                "{\"bidQ\":\"375\",\"bidP\":306.9,\"askQ\":\"150\",\"askP\":310.0}," +
                "{\"bidQ\":\"75\",\"bidP\":306.5,\"askQ\":\"225\",\"askP\":311.6}," +
                "{\"bidQ\":\"300\",\"bidP\":305.2,\"askQ\":\"375\",\"askP\":315.6}," +
                "{\"bidQ\":\"75\",\"bidP\":305.0,\"askQ\":\"150\",\"askP\":315.85}," +
                "{\"bidQ\":\"75\",\"bidP\":304.0,\"askQ\":\"150\",\"askP\":315.9}" +
                "] } } }" +
                "}";

        String tick2 = "{" +
                "\"fullFeed\": { \"marketFF\": {" +
                "\"ltpc\": {\"ltp\":320.0,\"ltq\":\"1000\"}," +
                "\"marketLevel\": { \"bidAskQuote\": [" +
                "{\"bidQ\":\"1000\",\"bidP\":320.0,\"askQ\":\"100\",\"askP\":321.0}" +
                "] } } }" +
                "}";

        QuantAnalysisService svc = new QuantAnalysisService();
        JsonNode j1 = mapper.readTree(tick1);
        JsonNode j2 = mapper.readTree(tick2);

        QuantAnalysisService.QuantAnalysisResult r1 = svc.analyze("NSE_FO|47161", j1);
        assertEquals(QuantAnalysisService.Signal.EXIT, r1.signal());

        QuantAnalysisService.QuantAnalysisResult r2 = svc.analyze("NSE_FO|47161", j2);
        assertEquals(QuantAnalysisService.Signal.ENTRY_LONG, r2.signal());
        assertTrue(r2.momentum() > 0);
        assertTrue(r2.volumeSpike() > 1.5);
        assertTrue(r2.imbalance() > 0.2);
    }
}

