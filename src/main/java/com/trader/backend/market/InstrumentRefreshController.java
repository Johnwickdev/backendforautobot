package com.trader.backend.market;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.bulk.BulkWriteResult;
import com.trader.backend.entity.NseInstrument;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;

@RestController
@RequestMapping("/api/instruments")
@RequiredArgsConstructor
@Slf4j
public class InstrumentRefreshController {

    private static final TypeReference<List<NseInstrument>> LIST_TYPE = new TypeReference<>() {
    };

    private final MongoTemplate mongoTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.instruments.nse-json:}")
    private String instrumentsPath;

    @PostMapping("/refresh")
    public MarketDtos.RefreshResult refresh() {
        try {
            List<NseInstrument> all = loadInstruments();
            int total = all.size();
            List<NseInstrument> filtered = all.stream()
                    .filter(this::isEligible)
                    .toList();
            ensureIndexes();
            if (filtered.isEmpty()) {
                log.info("event=INSTRUMENT_REFRESH_COMPLETED processed=0 upserted=0 skipped={}", total);
                return new MarketDtos.RefreshResult(0, 0, total);
            }

            BulkOperations ops = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, NseInstrument.class);
            int scheduled = 0;
            for (NseInstrument instrument : filtered) {
                if (!StringUtils.hasText(instrument.getInstrumentKey())) {
                    continue;
                }
                Document document = new Document();
                mongoTemplate.getConverter().write(instrument, document);
                Object id = document.remove("_id");
                if (id == null) {
                    id = instrument.getInstrumentKey();
                }
                Query query = Query.query(Criteria.where("_id").is(id));
                ops.upsert(query, Update.fromDocument(document));
                scheduled++;
            }

            int upserted = 0;
            if (scheduled > 0) {
                BulkWriteResult result = ops.execute();
                upserted = result.getUpserts().size() + result.getModifiedCount();
            }
            int processed = filtered.size();
            int skipped = total - processed;
            log.info("event=INSTRUMENT_REFRESH_COMPLETED processed={} upserted={} skipped={}", processed, upserted, skipped);
            return new MarketDtos.RefreshResult(processed, upserted, skipped);
        } catch (IOException e) {
            log.error("event=INSTRUMENT_REFRESH_FAILED reason={}", e.getMessage(), e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to refresh instruments");
        }
    }

    private List<NseInstrument> loadInstruments() throws IOException {
        try (InputStream inputStream = resolveInputStream()) {
            return objectMapper.readValue(inputStream, LIST_TYPE);
        }
    }

    private InputStream resolveInputStream() throws IOException {
        if (StringUtils.hasText(instrumentsPath)) {
            Path path = Path.of(instrumentsPath);
            if (Files.exists(path)) {
                log.info("Loading instruments from configured path: {}", path);
                return Files.newInputStream(path);
            }
            log.warn("Configured NSE.json path not found: {}", path);
        }
        InputStream resourceStream = InstrumentRefreshController.class.getClassLoader().getResourceAsStream("data/NSE.json");
        if (resourceStream != null) {
            return resourceStream;
        }
        throw new FileNotFoundException("Unable to locate NSE.json");
    }

    private boolean isEligible(NseInstrument instrument) {
        if (instrument == null) {
            return false;
        }
        String segment = instrument.getSegment();
        if (!StringUtils.hasText(segment)) {
            return false;
        }
        if ("NSE_EQ".equalsIgnoreCase(segment)) {
            return true;
        }
        if ("NSE_FO".equalsIgnoreCase(segment)) {
            String underlying = instrument.getUnderlyingSymbol();
            String name = instrument.getName();
            return (StringUtils.hasText(underlying) && underlying.equalsIgnoreCase("NIFTY"))
                    || (StringUtils.hasText(name) && name.toUpperCase(Locale.ROOT).contains("NIFTY"));
        }
        if ("NSE_INDEX".equalsIgnoreCase(segment)) {
            String name = instrument.getName();
            return StringUtils.hasText(name) && name.toUpperCase(Locale.ROOT).contains("NIFTY");
        }
        return false;
    }

    private void ensureIndexes() {
        IndexOperations indexOps = mongoTemplate.indexOps(NseInstrument.class);
        indexOps.ensureIndex(new Index().on("expiry", Sort.Direction.ASC));
        indexOps.ensureIndex(new Index().on("instrument_type", Sort.Direction.ASC));
        indexOps.ensureIndex(new Index().on("underlying_key", Sort.Direction.ASC));
        indexOps.ensureIndex(new Index().on("trading_symbol", Sort.Direction.ASC));
    }
}
