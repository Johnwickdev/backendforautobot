package com.trader.backend.market;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "candles")
public class Candle {

    @Id
    private String id;

    @Indexed
    private String instrumentKey;

    private String unit;

    private int interval;

    @Indexed
    private Instant ts;

    private double open;

    private double high;

    private double low;

    private double close;

    private long volume;

    private long openInterest;

    public static Candle create(String instrumentKey,
                                String unit,
                                int interval,
                                Instant ts,
                                double open,
                                double high,
                                double low,
                                double close,
                                long volume,
                                long openInterest) {
        String id = instrumentKey + "/" + ts.toEpochMilli();
        return Candle.builder()
                .id(id)
                .instrumentKey(instrumentKey)
                .unit(unit)
                .interval(interval)
                .ts(ts)
                .open(open)
                .high(high)
                .low(low)
                .close(close)
                .volume(volume)
                .openInterest(openInterest)
                .build();
    }
}
