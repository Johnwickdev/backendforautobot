package com.trader.backend.service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;
import com.trader.backend.dto.OptionType;
import com.trader.backend.dto.TradeRow;
import com.trader.backend.entity.NseInstrument;
import com.trader.backend.repository.NseInstrumentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class TradeHistoryService {
    private final LiveFeedService liveFeedService;
    private final NseInstrumentService nseInstrumentService;
    private final InfluxDBClient influxDBClient;
    private final NseInstrumentRepository repo;

    @Value("${influx.bucket:}")
    private String influxBucket;
    @Value("${influx.org:}")
    private String influxOrg;

    public record Result(List<TradeRow> rows, String source) {}

    public Optional<Result> fetchRecentOptionTrades(int limit, String side) {
        NseInstrumentService.SelectionData sel = nseInstrumentService.currentSelectionData();
        List<String> keys = sel.keys();
        if (keys == null || keys.isEmpty()) {
            return Optional.empty();
        }
        List<NseInstrument> instruments = new ArrayList<>();
        for (String key : keys) {
            repo.findById(key).ifPresent(instruments::add);
        }
        List<NseInstrument> options = instruments.stream()
                .filter(i -> {
                    String t = i.getInstrumentType();
                    return t != null && (t.equalsIgnoreCase("CE") || t.equalsIgnoreCase("PE"));
                })
                .toList();
        if (options.isEmpty()) {
            return Optional.empty();
        }
        String sideUp = side.toUpperCase();
        List<NseInstrument> filtered = options.stream()
                .filter(i -> {
                    String t = i.getInstrumentType();
                    if ("CE".equals(sideUp)) return "CE".equalsIgnoreCase(t);
                    if ("PE".equals(sideUp)) return "PE".equalsIgnoreCase(t);
                    return true;
                })
                .toList();
        if (filtered.isEmpty()) {
            return Optional.of(new Result(List.of(), "live"));
        }
        List<String> symbols = filtered.stream()
                .map(NseInstrument::getTradingSymbol)
                .filter(Objects::nonNull)
                .toList();

        List<TradeRow> rows = fetchFromLive(symbols, limit);
        String src = "live";
        if (rows.isEmpty()) {
            rows = fetchFromInflux(symbols, limit);
            src = rows.isEmpty() ? "none" : "influx";
        }
        return Optional.of(new Result(rows, src));
    }

    private List<TradeRow> fetchFromLive(List<String> symbols, int limit) {
        List<TradeRow> out = new ArrayList<>();
        for (String symbol : symbols) {
            List<LiveFeedService.OptTick> ticks = liveFeedService.recentOptionTicks(symbol);
            if (ticks.isEmpty()) continue;
            for (int i = 0; i < ticks.size(); i++) {
                LiveFeedService.OptTick t = ticks.get(i);
                LiveFeedService.OptTick prev = (i + 1 < ticks.size()) ? ticks.get(i + 1) : null;
                Double changePct = null;
                if (prev != null && prev.ltp() != 0) {
                    changePct = ((t.ltp() - prev.ltp()) / prev.ltp()) * 100.0;
                    changePct = Math.round(changePct * 100.0) / 100.0;
                }
                OptionType type = symbol.endsWith("PE") ? OptionType.PE : OptionType.CE;
                int strike = parseStrike(symbol);
                String txId = t.instrumentKey() + "-" + t.ts().getEpochSecond();
                Integer qty = t.qty() > 0 ? t.qty() : null;
                Integer oi = t.oi() > 0 ? t.oi() : null;
                out.add(new TradeRow(t.ts(), t.instrumentKey(), type, strike, t.ltp(), changePct, qty, oi, txId));
            }
        }
        out.sort(Comparator.comparing(TradeRow::ts).reversed());
        if (out.size() > limit) {
            return new ArrayList<>(out.subList(0, limit));
        }
        return out;
    }

    private List<TradeRow> fetchFromInflux(List<String> symbols, int limit) {
        if (symbols.isEmpty()) return List.of();
        String set = String.join(",", symbols.stream().map(s -> "\"" + s + "\"").toList());
        String flux = String.format("from(bucket: \"%s\") |> range(start: -1d) |> " +
                        "filter(fn: (r) => r._measurement == \"nifty_option_ticks\") |> " +
                        "filter(fn: (r) => contains(value: r.symbol, set: [%s])) |> " +
                        "filter(fn: (r) => r._field == \"ltp\" or r._field == \"qty\" or r._field == \"oi\") |> " +
                        "pivot(rowKey:[\"_time\",\"symbol\",\"instrumentKey\"], columnKey:[\"_field\"], valueColumn:\"_value\") |> " +
                        "sort(columns:[\"_time\"], desc:true) |> limit(n:%d)",
                influxBucket, set, limit * 3);

        QueryApi queryApi = influxDBClient.getQueryApi();
        List<FluxTable> tables = new ArrayList<>();
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                tables = queryApi.query(flux, influxOrg);
                break;
            } catch (Exception e) {
                long sleep = switch (attempt) {
                    case 0 -> 200L;
                    case 1 -> 500L;
                    default -> 1000L;
                };
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException ignored) {
                }
                if (attempt == 2) {
                    return List.of();
                }
            }
        }
        Map<String, List<FluxRecord>> bySymbol = new HashMap<>();
        for (FluxTable table : tables) {
            for (FluxRecord rec : table.getRecords()) {
                String symbol = (String) rec.getValueByKey("symbol");
                bySymbol.computeIfAbsent(symbol, k -> new ArrayList<>()).add(rec);
            }
        }
        List<TradeRow> out = new ArrayList<>();
        for (var entry : bySymbol.entrySet()) {
            List<FluxRecord> recs = entry.getValue();
            for (int i = 0; i < recs.size(); i++) {
                FluxRecord rec = recs.get(i);
                FluxRecord prev = (i + 1 < recs.size()) ? recs.get(i + 1) : null;
                Double ltp = getDouble(rec, "ltp");
                if (ltp == null) continue;
                Double prevLtp = prev != null ? getDouble(prev, "ltp") : null;
                Double changePct = null;
                if (prevLtp != null && prevLtp != 0.0) {
                    changePct = ((ltp - prevLtp) / prevLtp) * 100.0;
                    changePct = Math.round(changePct * 100.0) / 100.0;
                }
                Integer qty = getDouble(rec, "qty") != null && getDouble(rec, "qty").intValue() > 0
                        ? getDouble(rec, "qty").intValue() : null;
                Integer oi = getDouble(rec, "oi") != null && getDouble(rec, "oi").intValue() > 0
                        ? getDouble(rec, "oi").intValue() : null;
                String instrumentKey = (String) rec.getValueByKey("instrumentKey");
                Instant ts = (Instant) rec.getValueByKey("_time");
                String symbol = entry.getKey();
                OptionType type = symbol.endsWith("PE") ? OptionType.PE : OptionType.CE;
                int strike = parseStrike(symbol);
                String txId = instrumentKey + "-" + ts.getEpochSecond();
                out.add(new TradeRow(ts, instrumentKey, type, strike, ltp, changePct, qty, oi, txId));
            }
        }
        out.sort(Comparator.comparing(TradeRow::ts).reversed());
        if (out.size() > limit) {
            return new ArrayList<>(out.subList(0, limit));
        }
        return out;
    }

    private Double getDouble(FluxRecord rec, String field) {
        Object v = rec.getValueByKey(field);
        return v instanceof Number n ? n.doubleValue() : null;
    }

    private int parseStrike(String symbol) {
        int i = symbol.length() - 1;
        while (i >= 0 && !Character.isDigit(symbol.charAt(i))) {
            i--;
        }
        int end = i + 1;
        while (i >= 0 && Character.isDigit(symbol.charAt(i))) {
            i--;
        }
        try {
            return Integer.parseInt(symbol.substring(i + 1, end));
        } catch (Exception e) {
            return 0;
        }
    }
}
