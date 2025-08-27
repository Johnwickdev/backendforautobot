package com.trader.backend.service;

import com.trader.backend.entity.NseInstrument;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class NseInstrumentServiceTest {
    private NseInstrumentService svc = new NseInstrumentService(null, null, null, null, new ObjectMapper(), null);

    @Test
    void picksEarliestNonExpired() {
        ZoneId ist = ZoneId.of("Asia/Kolkata");
        LocalDate today = LocalDate.now(ist);
        NseInstrument expired = inst("k1", today.minusMonths(1));
        NseInstrument current = inst("k2", today.plusDays(5));
        NseInstrument next = inst("k3", today.plusMonths(1));
        Optional<NseInstrument> opt = svc.selectCurrentNiftyFuture(List.of(expired, current, next));
        assertTrue(opt.isPresent());
        assertEquals("k2", opt.get().getInstrument_key());
    }

    @Test
    void skipsExpiredMonth() {
        ZoneId ist = ZoneId.of("Asia/Kolkata");
        LocalDate today = LocalDate.now(ist);
        NseInstrument expired = inst("k1", today.minusDays(1));
        NseInstrument next = inst("k2", today.plusMonths(1));
        NseInstrument further = inst("k3", today.plusMonths(2));
        Optional<NseInstrument> opt = svc.selectCurrentNiftyFuture(List.of(expired, next, further));
        assertTrue(opt.isPresent());
        assertEquals("k2", opt.get().getInstrument_key());
    }

    private static NseInstrument inst(String key, LocalDate date) {
        NseInstrument i = new NseInstrument();
        i.setInstrument_key(key);
        long exp = date.atStartOfDay(ZoneId.of("Asia/Kolkata")).toInstant().toEpochMilli();
        i.setExpiry(exp);
        return i;
    }
}
