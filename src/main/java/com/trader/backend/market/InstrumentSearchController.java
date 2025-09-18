package com.trader.backend.market;

import com.trader.backend.entity.NseInstrument;
import com.trader.backend.service.NseInstrumentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.regex.Pattern;

@RestController
@RequestMapping("/api/instruments")
@RequiredArgsConstructor
@Slf4j
public class InstrumentSearchController {

    private static final int MAX_LIMIT = 100;
    private static final String COLLECTION = NseInstrumentService.INSTRUMENT_SEARCH_COLLECTION;

    private final MongoTemplate mongoTemplate;

    @GetMapping("/search")
    public List<MarketDtos.InstrumentHit> search(@RequestParam("q") String query,
                                                 @RequestParam(value = "limit", defaultValue = "20") int limit) {
        if (!StringUtils.hasText(query)) {
            return List.of();
        }
        int sanitizedLimit = Math.min(Math.max(limit, 1), MAX_LIMIT);
        String sanitizedQuery = query.trim();
        Pattern pattern = Pattern.compile(Pattern.quote(sanitizedQuery), Pattern.CASE_INSENSITIVE);
        Criteria criteria = new Criteria().orOperator(
                Criteria.where("trading_symbol").regex(pattern),
                Criteria.where("name").regex(pattern)
        );
        Query mongoQuery = Query.query(criteria)
                .limit(sanitizedLimit)
                .with(Sort.by(Sort.Direction.ASC, "trading_symbol"));
        List<NseInstrument> results = mongoTemplate.find(mongoQuery, NseInstrument.class, COLLECTION);
        return results.stream()
                .map(this::toDto)
                .toList();
    }

    private MarketDtos.InstrumentHit toDto(NseInstrument instrument) {
        String display = StringUtils.hasText(instrument.getName()) ? instrument.getName() : instrument.getTradingSymbol();
        String symbol = StringUtils.hasText(instrument.getTradingSymbol()) ? instrument.getTradingSymbol() : instrument.getAssetSymbol();
        Long expiry = instrument.getExpiry() > 0 ? instrument.getExpiry() : null;
        return new MarketDtos.InstrumentHit(
                display,
                instrument.getInstrumentKey(),
                instrument.getSegment(),
                symbol,
                expiry,
                instrument.getStrikePrice(),
                instrument.getInstrumentType()
        );
    }
}
