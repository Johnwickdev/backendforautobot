package com.trader.backend.service;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.util.Iterator;
import java.time.Duration;
import reactor.core.publisher.Mono;

@Component
@Profile("mock")
@RequiredArgsConstructor
@Slf4j
public class MockTickRunner implements CommandLineRunner {

    private final QuantAnalysisService quantAnalysisService;
    private final SyntheticTickGenerator syntheticTickGenerator;
    private final MarketStatusService marketStatusService;

    @Override
    public void run(String... args) throws Exception {
        for (int i = 0; i < 20; i++) {
            JsonNode tick = syntheticTickGenerator.next();
            JsonNode feeds = tick.path("feeds");
            Iterator<String> it = feeds.fieldNames();
            QuantAnalysisService.QuantAnalysisResult futuresResult = null;
            while (it.hasNext()) {
                String key = it.next();
                JsonNode feed = feeds.get(key);
                QuantAnalysisService.QuantAnalysisResult r =
                        quantAnalysisService.analyze(key, feed);
                log.info("\uD83E\uDD14 [{}] momentum={} volSpike={} imbalance={} noise={} signal={}",
                        key,
                        String.format("%.4f", r.momentum()),
                        String.format("%.2f", r.volumeSpike()),
                        String.format("%.2f", r.imbalance()),
                        String.format("%.2f", r.noise()),
                        r.signal());
                if (r.signal() == QuantAnalysisService.Signal.ENTRY_LONG
                        || r.signal() == QuantAnalysisService.Signal.ENTRY_SHORT) {
                    log.info("⏱ mock entry detected for {} — scheduling exit in 1s", key);
                    Mono.delay(Duration.ofSeconds(1))
                            .subscribe(v -> log.info("⏳ mock exit for {}", key));
                }
                if ("NSE_FO|64103".equals(key)) {
                    futuresResult = r;
                }
            }
            if (futuresResult != null) {
                MarketStatusService.MarketStatus status = marketStatusService.determine(futuresResult);
                log.info("\uD83D\uDCCA Market status: {}", status);
            }
            Thread.sleep(200); // simulate tick interval
        }
    }
}
