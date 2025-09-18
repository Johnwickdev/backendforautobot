package com.trader.backend.market;

import org.springframework.data.mongodb.repository.MongoRepository;

import java.time.Instant;
import java.util.List;

public interface CandleRepository extends MongoRepository<Candle, String> {

    List<Candle> findByInstrumentKeyAndUnitAndIntervalAndTsBetween(String instrumentKey,
                                                                   String unit,
                                                                   int interval,
                                                                   Instant from,
                                                                   Instant to);
}
