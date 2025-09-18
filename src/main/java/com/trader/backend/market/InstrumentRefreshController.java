package com.trader.backend.market;

import com.trader.backend.market.MarketDtos;
import com.trader.backend.service.NseInstrumentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/instruments")
@RequiredArgsConstructor
@Slf4j
public class InstrumentRefreshController {

    private final NseInstrumentService nseInstrumentService;

    @PostMapping("/refresh")
    public MarketDtos.RefreshResult refresh() {
        try {
            return nseInstrumentService.refreshInstrumentSearchUniverse();
        } catch (IllegalStateException e) {
            log.error("event=INSTRUMENT_REFRESH_FAILED reason={}", e.getMessage(), e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to refresh instruments");
        }
    }
}
