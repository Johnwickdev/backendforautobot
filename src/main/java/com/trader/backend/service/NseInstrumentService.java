package com.trader.backend.service;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.temporal.TemporalAdjusters;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.http.HttpHeaders;
import reactor.core.publisher.Mono;
import org.springframework.beans.factory.annotation.Value;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import com.trader.backend.entity.NseInstrument;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Criteria;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.trader.backend.entity.NseInstrument;
import com.trader.backend.repository.NseInstrumentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import org.springframework.context.annotation.Lazy;
// imports to add at the top of NseInstrumentService
import org.springframework.context.ApplicationEventPublisher;
import com.trader.backend.events.FilteredPremiumsUpdatedEvent;




@RequiredArgsConstructor
@Slf4j
@Service
public class NseInstrumentService {
    // in class fields:
    private final ApplicationEventPublisher publisher;

    private final WebClient webClient;
    private final UpstoxAuthService upstoxAuthService;
    private final NseInstrumentRepository repo;
    private final ObjectMapper mapper;
    private final MongoTemplate mongoTemplate;

    // Cache for NSE.json contents
    private List<NseInstrument> nseCache = Collections.emptyList();
    private long nseCacheLoadedAt = 0L;
    private static final long CACHE_TTL_MS = 24 * 60 * 60 * 1000L; // 24h

    // ensure heavy CE/PE filtering runs once
    private final AtomicBoolean strikesFiltered = new AtomicBoolean(false);

    /** Ensure NSE.json is loaded into memory. */
    public synchronized void ensureNseJsonLoaded(boolean force) {
        long now = System.currentTimeMillis();
        if (!force && !nseCache.isEmpty() && (now - nseCacheLoadedAt) < CACHE_TTL_MS) {
            return;
        }
        try {
            File jsonFile = new File("src/main/resources/data/NSE.json");
            nseCache = Arrays.asList(mapper.readValue(jsonFile, NseInstrument[].class));
            nseCacheLoadedAt = now;
            log.debug("NSE.json parsed ({} records)", nseCache.size());
        } catch (IOException e) {
            log.error("Failed to load NSE.json", e);
        }
    }

    public void ensureNseJsonLoaded() {
        ensureNseJsonLoaded(false);
    }

    public List<NseInstrument> getCachedNse() {
        ensureNseJsonLoaded(false);
        return nseCache;
    }

    public void saveFilteredInstrumentsToMongo() {
        try {
            log.info("📂 Reading NSE.json for filtering...");
            File jsonFile = new File("src/main/resources/data/NSE.json");

            List<NseInstrument> allInstruments = Arrays.asList(mapper.readValue(jsonFile, NseInstrument[].class));

            log.info("🔍 Filtering only NIFTY CE/PE between ₹10 and ₹20 (simulated)...");

            List<NseInstrument> filtered = allInstruments.stream()
                    .filter(ins -> "NSE_FO".equals(ins.getSegment()))
                    .filter(ins -> "NIFTY".equals(ins.getName()))
                    .filter(ins -> {
                        String type = ins.getInstrumentType();
                        return "CE".equals(type) || "PE".equals(type);
                    })
                    .filter(ins -> {
                        // NOTE: Actual premium filtering must use live LTP; here we just simulate by strike range
                        double strike = ins.getStrikePrice();
                        return strike >= 23000 && strike <= 24000;
                    })
                    .collect(Collectors.toList());

            log.info("💾 Saving filtered instruments: {}", filtered.size());
            repo.saveAll(filtered);
            log.info("✅ Done saving filtered NIFTY CE/PE instruments to MongoDB.");

        } catch (Exception e) {
            log.error("❌ Failed during filtering/saving: ", e);
        }
    }


public void filterAndSaveStrikesAroundLtp(double niftyLtp) {
    try {
        log.info("📊 Calculating strike range around LTP: {}", niftyLtp);

        double baseStrike = Math.round(niftyLtp / 50.0) * 50;

        // 🎯 Create set of target strikes from baseStrike - 750 to +750 (15 each side)
        Set<Double> targetStrikes = new HashSet<>();
        for (int i = -15; i <= 15; i++) {
            targetStrikes.add(baseStrike + (i * 50));
        }

        // 🔍 Load NSE.json and parse
        InputStream jsonStream = new FileInputStream("src/main/resources/data/NSE.json"); // 🔄 Fixed path
        ObjectMapper mapper = new ObjectMapper();
        List<NseInstrument> all = Arrays.asList(mapper.readValue(jsonStream, NseInstrument[].class));

        // ⚙️ Filter CE and PE instruments for NIFTY
        List<NseInstrument> filtered = all.stream()
            .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
            .filter(i -> "CE".equals(i.getInstrumentType()) || "PE".equals(i.getInstrumentType()))
            .filter(i -> targetStrikes.contains(i.getStrikePrice()))
            .collect(Collectors.toList());
mongoTemplate.dropCollection("filtered_nifty_premiums");
mongoTemplate.insert(filtered, "filtered_nifty_premiums");
log.info("✅ CE/PE filtering and save complete ✅");

// pick the most frequent expiry among saved docs (safety if mixed)
long activeExpiry = filtered.stream()
        .collect(java.util.stream.Collectors.groupingBy(NseInstrument::getExpiry, java.util.stream.Collectors.counting()))
        .entrySet().stream()
        .max(java.util.Map.Entry.comparingByValue())
        .map(java.util.Map.Entry::getKey)
        .orElseGet(() -> filtered.get(0).getExpiry());

// 🔔 Notify LiveFeedService
publisher.publishEvent(new FilteredPremiumsUpdatedEvent(this, activeExpiry, filtered.size()));
          } catch (Exception e) {
        log.error("❌ Error during CE/PE strike filtering and saving", e);
    }
}
  /**
 * Select 10 CE + 10 PE around base(=round(ltp/50)*50) for the ACTIVE expiry stored in nse_instruments.
 * If the universe is empty, we auto-refresh it from NSE.json by nearest expiry.
 */
public void filterStrikesAroundLtp(double niftyLtp) {
    if (!strikesFiltered.compareAndSet(false, true)) {
        return;
    }
    log.info("🔎 Filtering CE/PE around LTP={} (step=50) for currently loaded expiry in nse_instruments", niftyLtp);

    // 0) Ensure universe exists in DB; if not, refresh from JSON
    Query existsQ = new Query(Criteria.where("underlying_key").is("NSE_INDEX|Nifty 50")
            .and("segment").is("NSE_FO")
            .and("instrumentType").in("CE", "PE"));
    long count = mongoTemplate.count(existsQ, "nse_instruments");
    if (count == 0) {
        log.warn("⚠️ nse_instruments is empty. Refreshing from JSON by nearest expiry...");
        refreshNiftyOptionsByNearestExpiryFromJson();
        count = mongoTemplate.count(existsQ, "nse_instruments");
        if (count == 0) {
            log.error("❌ Still empty after refresh. Aborting CE/PE filtering.");
            return;
        }
    }

    // 1) Read active expiry from nse_instruments (the single expiry we just loaded)
    List<Long> expiries = mongoTemplate.findDistinct(new Query(), "expiry", "nse_instruments", Long.class);
    if (expiries.isEmpty() || isExpiryCompleted(expiries.get(0))) {
        long expired = expiries.isEmpty() ? -1L : expiries.get(0);
        log.warn("⚠️ Active expiry={} is past cutoff. Refreshing from JSON...", expired);
        refreshNiftyOptionsByNearestExpiryFromJson();
        expiries = mongoTemplate.findDistinct(new Query(), "expiry", "nse_instruments", Long.class);
        if (expiries.isEmpty()) {
            log.error("❌ No expiry found in nse_instruments after refresh.");
            return;
        }
    }
    long activeExpiry = expiries.stream().sorted().findFirst().get();
    log.info("🗓️ Active expiry epoch={} (ms) will be used for selection", activeExpiry);

    // 2) Build strike set around LTP (ATM, 1 ITM, rest OTM – you can tweak counts)
    double base = Math.round(niftyLtp / 50.0) * 50;
    // We'll allow a window of 30 strikes each way, but we will eventually limit to 10/side.
    Set<Integer> strikeSet = new LinkedHashSet<>();
    for (int i = -30; i <= 30; i++) strikeSet.add((int)(base + i * 50));

    // 3) Pull candidates only for that expiry
    Query q = new Query(Criteria.where("underlying_key").is("NSE_INDEX|Nifty 50")
            .and("segment").is("NSE_FO")
            .and("instrumentType").in("CE", "PE")
            .and("expiry").is(activeExpiry)
            .and("strikePrice").in(strikeSet));
    List<NseInstrument> pool = mongoTemplate.find(q, NseInstrument.class, "nse_instruments");
    log.info("📊 Candidates pool size={} for base={}, expiry={}", pool.size(), base, activeExpiry);

    // 4) Sort by distance from base, then by strike
    Comparator<NseInstrument> byDistance = Comparator
            .comparingDouble((NseInstrument i) -> Math.abs(i.getStrikePrice() - base))
            .thenComparingDouble(NseInstrument::getStrikePrice);

    List<NseInstrument> ceSorted = pool.stream()
            .filter(i -> "CE".equals(i.getInstrumentType()))
            .sorted(byDistance)
            .collect(Collectors.collectingAndThen(
                    Collectors.toMap(NseInstrument::getStrikePrice, i -> i, (a,b)->a, LinkedHashMap::new),
                    m -> new ArrayList<>(m.values())
            ));
    List<NseInstrument> peSorted = pool.stream()
            .filter(i -> "PE".equals(i.getInstrumentType()))
            .sorted(byDistance)
            .collect(Collectors.collectingAndThen(
                    Collectors.toMap(NseInstrument::getStrikePrice, i -> i, (a,b)->a, LinkedHashMap::new),
                    m -> new ArrayList<>(m.values())
            ));

    // 5) Take 10 + 10 (adjust if you want 15 + 15)
    int CE_COUNT = 10, PE_COUNT = 10;
    List<NseInstrument> selected = new ArrayList<>();
    selected.addAll(ceSorted.stream().limit(CE_COUNT).toList());
    selected.addAll(peSorted.stream().limit(PE_COUNT).toList());

    if (selected.isEmpty()) {
        log.warn("⚠️ No instruments selected for filtered_nifty_premiums (expiry={}).", activeExpiry);
        return;
    }

    // 6) Save selection to filtered_nifty_premiums
    mongoTemplate.dropCollection("filtered_nifty_premiums");
    mongoTemplate.insert(selected, "filtered_nifty_premiums");
    log.info("✅ Saved {} instruments ({} CE + {} PE) to filtered_nifty_premiums for expiry={}",
            selected.size(), Math.min(CE_COUNT, ceSorted.size()), Math.min(PE_COUNT, peSorted.size()), activeExpiry);

    // 🔔 Notify LiveFeedService via event (no circular dependency)
    publisher.publishEvent(new FilteredPremiumsUpdatedEvent(this, activeExpiry, selected.size()));
}

    public void saveAllNiftyOptionsFromJson() {
        log.info("📂 Reading NSE.json to save all NIFTY CE/PE options...");

        try {
            File jsonFile = new File("src/main/resources/data/NSE.json");
            List<NseInstrument> all = mapper.readValue(jsonFile, new TypeReference<>() {});

            // Filter only CE/PE where underlying is Nifty 50
            List<NseInstrument> niftyOptions = all.stream()
                    .filter(inst ->
                            "NSE_INDEX|Nifty 50".equals(inst.getUnderlying_key()) &&
                                    "NSE_FO".equals(inst.getSegment()) &&
                                    ("CE".equals(inst.getInstrumentType()) || "PE".equals(inst.getInstrumentType()))
                    )
                    .toList();

            log.info("✅ Total NIFTY CE/PE instruments found: {}", niftyOptions.size());

            mongoTemplate.dropCollection("nse_instruments");
            mongoTemplate.insertAll(niftyOptions);
            log.info("💾 Saved to 'nse_instruments' collection.");

        } catch (IOException e) {
            log.error("❌ Error reading NSE.json or saving to MongoDB", e);
        }
    }
   // NseInstrumentService.java
public List<String> getInstrumentKeysForLiveSubscription() {
    long now = System.currentTimeMillis();

    // 1) What expiries are present in filtered_nifty_premiums (future only)?
    List<Long> expiries = mongoTemplate.findDistinct(
            new Query(Criteria.where("expiry").gte(now)),
            "expiry",
            "filtered_nifty_premiums",
            Long.class
    );

    if (expiries.isEmpty()) {
        log.warn("⚠️ filtered_nifty_premiums has no future expiries. Did filtering run?");
        return List.of();
    }

    // 2) Pick the nearest future expiry actually stored (e.g., 14-Aug)
    long targetExpiry = expiries.stream().sorted().findFirst().get();
    var targetDayIST  = Instant.ofEpochMilli(targetExpiry).atZone(IST).toLocalDate();
    long dayStart = istStartOfDayMs(targetDayIST);
    long dayEnd   = istEndOfDayMs(targetDayIST);

    // 3) Pull the 20 docs for that day and extract keys
    Query q = new Query(new Criteria().andOperator(
            Criteria.where("expiry").gte(dayStart).lt(dayEnd)
    ));
    List<NseInstrument> docs =
            mongoTemplate.find(q, NseInstrument.class, "filtered_nifty_premiums");

    List<String> keys = new ArrayList<>(
            docs.stream()
                .map(NseInstrument::getInstrument_key)
                .filter(Objects::nonNull)
                .distinct()
                .toList()
    );

    // append nearest NIFTY FUT instrument key
    nearestNiftyFutureKey().ifPresent(futKey -> {
        if (!keys.contains(futKey)) keys.add(futKey);
    });

    log.info("🔑 Prepared {} keys for live stream | expiryDay(IST)={} | epoch={}",
            keys.size(), targetDayIST, targetExpiry);

    if (keys.isEmpty()) {
        log.warn("⚠️ Expiry exists but no instrument keys were found in filtered_nifty_premiums.");
    }
    return keys;
}

public record SelectionData(long expiry, List<String> keys, int ceCount, int peCount) {}

public SelectionData currentSelectionData() {
    long now = System.currentTimeMillis();

    List<Long> expiries = mongoTemplate.findDistinct(
            new Query(Criteria.where("expiry").gte(now)),
            "expiry",
            "filtered_nifty_premiums",
            Long.class
    );
    if (expiries.isEmpty()) {
        return new SelectionData(0L, List.of(), 0, 0);
    }
    long targetExpiry = expiries.stream().sorted().findFirst().get();
    var targetDayIST = Instant.ofEpochMilli(targetExpiry).atZone(IST).toLocalDate();
    long dayStart = istStartOfDayMs(targetDayIST);
    long dayEnd = istEndOfDayMs(targetDayIST);

    Query q = new Query(new Criteria().andOperator(
            Criteria.where("expiry").gte(dayStart).lt(dayEnd)
    ));
    List<NseInstrument> docs =
            mongoTemplate.find(q, NseInstrument.class, "filtered_nifty_premiums");

    List<String> keys = new ArrayList<>(
            docs.stream()
                    .map(NseInstrument::getInstrument_key)
                    .filter(Objects::nonNull)
                    .distinct()
                    .toList()
    );

    int ce = (int) docs.stream().filter(i -> "CE".equalsIgnoreCase(i.getInstrumentType())).count();
    int pe = (int) docs.stream().filter(i -> "PE".equalsIgnoreCase(i.getInstrumentType())).count();

    nearestNiftyFutureKey().ifPresent(futKey -> {
        if (!keys.contains(futKey)) keys.add(futKey);
    });

    return new SelectionData(targetExpiry, keys, ce, pe);
}

public String selectionSignature(SelectionData sel) {
    List<String> sorted = new ArrayList<>(sel.keys());
    Collections.sort(sorted);
    String date = Instant.ofEpochMilli(sel.expiry()).atZone(IST).toLocalDate().toString();
    return date + ":" + sorted.hashCode();
}

public String formatExpiry(long expiry) {
    return Instant.ofEpochMilli(expiry).atZone(IST).toLocalDate().toString();
}
public void saveNiftyFuturesToMongo() {
    log.info("📂 Extracting NIFTY FUTURE records from NSE.json...");
    ensureNseJsonLoaded(false);
    List<NseInstrument> all = new ArrayList<>(nseCache);
    for (NseInstrument i : all) {
        if (i.getName() != null) i.setName(i.getName().trim());
        if (i.getSegment() != null) i.setSegment(i.getSegment().trim());
        if (i.getInstrumentType() != null) i.setInstrumentType(i.getInstrumentType().trim());
        if (i.getUnderlying_key() != null) i.setUnderlying_key(i.getUnderlying_key().trim());
    }
    // Step 1: Filter valid NIFTY FUTs
    List<NseInstrument> niftyFutures = all.stream()
            .filter(inst -> "FUT".equalsIgnoreCase(inst.getInstrumentType()))
            .filter(inst -> "NSE_FO".equalsIgnoreCase(inst.getSegment()))
            .filter(inst -> "NIFTY".equalsIgnoreCase(inst.getName()))
            .filter(inst -> !inst.isWeekly())
            .filter(inst -> inst.getLot_size() == 75)
            .filter(inst -> "NSE_INDEX|Nifty 50".equals(inst.getUnderlying_key()))
            .sorted(Comparator.comparingLong(NseInstrument::getExpiry))
            .toList();

    log.info("✅ Found {} NIFTY FUT contracts with lot size 75", niftyFutures.size());
    niftyFutures.forEach(i ->
            log.debug("📄 {} | expiry={} | key={}"
                    , i.getTrading_symbol(), i.getExpiry(), i.getInstrument_key())

    );

    // Step 2: Save to separate collection
    if (!niftyFutures.isEmpty()) {
        mongoTemplate.dropCollection("nifty_futures");
        mongoTemplate.insert(niftyFutures, "nifty_futures");
        mongoTemplate.indexOps("nifty_futures")
                .ensureIndex(new org.springframework.data.mongodb.core.index.Index().on("expiry", Sort.Direction.ASC));
        log.info("💾 Saved to MongoDB collection: nifty_futures (indexed on expiry)");
    } else {
        log.warn("⚠️ No matching NIFTY FUT records found.");
    }
}

// helper: choose nearest non-expired NIFTY FUT using IST 3:30 PM cutoff
Optional<NseInstrument> selectCurrentNiftyFuture(List<NseInstrument> futs) {
    if (futs == null || futs.isEmpty()) return Optional.empty();

    // de-duplicate by instrument_key
    Map<String, NseInstrument> unique = futs.stream()
            .collect(Collectors.toMap(NseInstrument::getInstrument_key, f -> f, (a, b) -> a));

    ZonedDateTime now = ZonedDateTime.now(IST);
    List<String> otherStatuses = new ArrayList<>();
    NseInstrument chosen = null;
    ZonedDateTime chosenExpiry = null;

    List<NseInstrument> sorted = unique.values().stream()
            .sorted(Comparator.comparingLong(NseInstrument::getExpiry))
            .toList();

    for (NseInstrument f : sorted) {
        LocalDate d = Instant.ofEpochMilli(f.getExpiry()).atZone(IST).toLocalDate();
        ZonedDateTime cutoff = d.atTime(EXPIRY_CUTOFF).atZone(IST);
        boolean expired = now.isAfter(cutoff);
        String month = extractMonth(f.getTrading_symbol());
        log.info("📄 {} | expiry={} IST {}", f.getTrading_symbol(), cutoff, expired ? "[expired]" : "[valid]");
        if (expired) {
            otherStatuses.add(month + " expired");
        } else if (chosen == null) {
            chosen = f;
            chosenExpiry = cutoff;
        } else {
            otherStatuses.add(month + " later");
        }
    }

    if (chosen == null) {
        log.warn("⚠️ No non-expired NIFTY FUT contract found.");
        return Optional.empty();
    }

    log.info("Chosen current NIFTY FUT = {} | expiry={} IST (picked as nearest non-expired vs [{}])",
            chosen.getTrading_symbol(),
            chosenExpiry,
            String.join(", ", otherStatuses));

    return Optional.of(chosen);
}

private String extractMonth(String tradingSymbol) {
    if (tradingSymbol == null) return "";
    String[] parts = tradingSymbol.split(" ");
    return parts.length >= 4 ? parts[3] : tradingSymbol;
}

public Mono<Double> getNearestExpiryNiftyFutureLtp() {
    // 1. Fetch all saved NIFTY FUT contracts
    Query query = new Query();
    query.addCriteria(Criteria.where("segment").is("NSE_FO")
            .and("instrumentType").is("FUT")
            .and("lot_size").is(75));  // Ensure it's real NIFTY, not BANKNIFTY or others

    List<NseInstrument> niftyFuts = mongoTemplate.find(query, NseInstrument.class, "nifty_futures");

    if (niftyFuts.isEmpty()) {
        log.error("❌ No NIFTY FUT found in DB.");
        return Mono.error(new RuntimeException("No NIFTY FUT data in DB"));
    }

    // 2. Choose nearest non-expired future using IST cutoff
    Optional<NseInstrument> current = selectCurrentNiftyFuture(niftyFuts);

    if (current.isEmpty()) {
        log.error("❌ No valid NIFTY FUT contract with future expiry.");
        return Mono.error(new RuntimeException("No future expiry NIFTY FUT found"));
    }

    String instrumentKey = current.get().getInstrument_key();
    log.info("📌 Selected NIFTY FUT key for LTP: " + instrumentKey);

    // 3. Call Upstox LTP API
    String url = "https://api.upstox.com/v2/market-quote/ltp?instrument_key=" + instrumentKey;

    return webClient.get()
            .uri(url)
            .header(HttpHeaders.AUTHORIZATION, "Bearer " + upstoxAuthService.currentToken())
            .header(HttpHeaders.ACCEPT, "application/json")
            .retrieve()
            .bodyToMono(String.class)
            .flatMap(response -> {
                // Parse LTP value from JSON (simplified)
                try {
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode root = mapper.readTree(response);
                    JsonNode ltpNode = root.path("data").path(instrumentKey).path("ltp");

                    if (!ltpNode.isMissingNode()) {
                        double ltp = ltpNode.asDouble();
                        log.info("📈 NIFTY FUT LTP: " + ltp);
                        return Mono.just(ltp);
                    } else {
                        return Mono.error(new RuntimeException("LTP not found in response"));
                    }
                } catch (Exception e) {
                    return Mono.error(e);
                }
            });
}

private void refreshNiftyFuturesIfNeeded() {
    long now = System.currentTimeMillis();
    Query q = new Query(Criteria.where("segment").is("NSE_FO")
            .and("instrumentType").is("FUT")
            .and("name").is("NIFTY")
            .and("expiry").gt(now));
    long count = mongoTemplate.count(q, "nifty_futures");
    if (count == 0) {
        log.warn("⚠️ No future NIFTY FUT contracts found. Reloading from JSON...");
        saveNiftyFuturesToMongo();
    }
}

public Optional<String> nearestNiftyFutureKey() {
    refreshNiftyFuturesIfNeeded();

    ZonedDateTime now = ZonedDateTime.now(IST);
    long nowMs = now.toInstant().toEpochMilli();

    // log first three futures to show decision trail
    Query logQ = new Query(new Criteria().andOperator(
            Criteria.where("segment").is("NSE_FO"),
            Criteria.where("instrumentType").is("FUT"),
            Criteria.where("name").is("NIFTY"),
            Criteria.where("underlying_key").is("NSE_INDEX|Nifty 50"),
            Criteria.where("weekly").is(false),
            Criteria.where("lot_size").is(75)
    )).with(Sort.by(Sort.Direction.ASC, "expiry")).limit(3);
    List<NseInstrument> top = mongoTemplate.find(logQ, NseInstrument.class, "nifty_futures");
    top.forEach(f -> {
        LocalDate d = Instant.ofEpochMilli(f.getExpiry()).atZone(IST).toLocalDate();
        ZonedDateTime cutoff = d.atTime(EXPIRY_CUTOFF).atZone(IST);
        boolean expired = now.isAfter(cutoff);
        log.info("📄 {} | expiry={} IST {}", f.getTrading_symbol(), cutoff, expired ? "[expired]" : "[valid]");
    });

    // query DB for nearest non-expired future
    Query q = new Query(new Criteria().andOperator(
            Criteria.where("segment").is("NSE_FO"),
            Criteria.where("instrumentType").is("FUT"),
            Criteria.where("name").is("NIFTY"),
            Criteria.where("underlying_key").is("NSE_INDEX|Nifty 50"),
            Criteria.where("weekly").is(false),
            Criteria.where("lot_size").is(75),
            Criteria.where("expiry").gte(nowMs)
    )).with(Sort.by(Sort.Direction.ASC, "expiry")).limit(1);

    NseInstrument current = mongoTemplate.findOne(q, NseInstrument.class, "nifty_futures");
    if (current == null) {
        log.error("❌ No valid NIFTY FUT contract with future expiry.");
        return Optional.empty();
    }

    // persist chosen contract using upsert
    mongoTemplate.save(current, "current_nifty_future");

    String chosenMonth = extractMonth(current.getTrading_symbol());
    List<String> reasons = new ArrayList<>();
    for (NseInstrument f : top) {
        if (f.getInstrument_key().equals(current.getInstrument_key())) continue;
        LocalDate d = Instant.ofEpochMilli(f.getExpiry()).atZone(IST).toLocalDate();
        ZonedDateTime cutoff = d.atTime(EXPIRY_CUTOFF).atZone(IST);
        String m = extractMonth(f.getTrading_symbol());
        if (now.isAfter(cutoff)) {
            reasons.add(m + " expired on " + cutoff + " IST");
        } else {
            reasons.add(m + " later (" + cutoff + " IST)");
        }
    }
    log.info("Chosen current NIFTY FUT = {} | expiry={} IST ({}).", current.getTrading_symbol(),
            Instant.ofEpochMilli(current.getExpiry()).atZone(IST), String.join("; ", reasons));

    return Optional.ofNullable(current.getInstrument_key());
}

public void refreshNiftyOptionsCurrentWeek() {
    log.info("🔁 Refreshing nse_instruments with CURRENT-WEEK NIFTY CE/PE from NSE.json...");
    try {
        File jsonFile = new File("src/main/resources/data/NSE.json");
        List<NseInstrument> all = mapper.readValue(jsonFile, new TypeReference<>() {});

        // normalize
        for (NseInstrument i : all) {
            if (i.getName() != null) i.setName(i.getName().trim());
            if (i.getSegment() != null) i.setSegment(i.getSegment().trim());
            if (i.getInstrumentType() != null) i.setInstrumentType(i.getInstrumentType().trim());
            if (i.getUnderlying_key() != null) i.setUnderlying_key(i.getUnderlying_key().trim());
        }

        long currentWeekExpiry = detectCurrentWeekExpiryEpoch(all);

        List<NseInstrument> currentWeek = all.stream()
                .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
                .filter(i -> "NSE_FO".equals(i.getSegment()))
                .filter(i -> "CE".equals(i.getInstrumentType()) || "PE".equals(i.getInstrumentType()))
                .filter(i -> i.getExpiry() == currentWeekExpiry)
                .toList();

        mongoTemplate.dropCollection("nse_instruments");
        mongoTemplate.insert(currentWeek, "nse_instruments");

        log.info("✅ nse_instruments refreshed with {} CURRENT-WEEK CE/PE", currentWeek.size());
    } catch (Exception e) {
        log.error("❌ Failed to refresh CURRENT-WEEK instruments", e);
    }
}

// NEW: tiny helper used by refreshNiftyOptionsCurrentWeek()
private long detectCurrentWeekExpiryEpoch(List<NseInstrument> all) {
    long now = System.currentTimeMillis();
    return all.stream()
            .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
            .filter(i -> "NSE_FO".equals(i.getSegment()))
            .filter(i -> "CE".equals(i.getInstrumentType()) || "PE".equals(i.getInstrumentType()))
            .mapToLong(NseInstrument::getExpiry)   // primitive long, no nulls
            .filter(exp -> exp >= now)
            .sorted()
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("No future expiry found in NSE.json"));
}
    //private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
private static final LocalTime EXPIRY_CUTOFF = LocalTime.of(15, 30); // 3:30 PM IST

private LocalDate resolveCurrentCycleExpiryWednesdayIST() {
    ZonedDateTime now = ZonedDateTime.now(IST);
    if (now.getDayOfWeek() == DayOfWeek.WEDNESDAY) {
        return now.toLocalTime().isAfter(EXPIRY_CUTOFF)
                ? now.toLocalDate().plusWeeks(1).with(DayOfWeek.WEDNESDAY)
                : now.toLocalDate();
    }
    return now.toLocalDate().with(TemporalAdjusters.next(DayOfWeek.WEDNESDAY));
}

private long istStartOfDayMs(LocalDate d) {
    return d.atStartOfDay(IST).toInstant().toEpochMilli();
}
private long istEndOfDayMs(LocalDate d) {
    return d.plusDays(1).atStartOfDay(IST).toInstant().toEpochMilli();
}
/**
 * Load CURRENT-CYCLE NIFTY CE/PE into nse_instruments using the nearest future expiry found in NSE.json.
 * No weekday heuristics. We trust the per-row "expiry" epoch.
 */
public void refreshNiftyOptionsByNearestExpiryFromJson() {
    log.info("🔁 Refreshing nse_instruments by nearest future expiry from NSE.json (NIFTY CE/PE)...");
    try {
        ensureNseJsonLoaded(false);
        List<NseInstrument> all = new ArrayList<>(nseCache);
        all.forEach(this::normalizeInstrument);

        long now = System.currentTimeMillis();

        // 1) Find nearest future expiry among NIFTY CE/PE contracts
        OptionalLong nearestExpiryOpt = all.stream()
                .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
                .filter(i -> "NSE_FO".equals(i.getSegment()))
                .filter(i -> {
                    String t = i.getInstrumentType();
                    return "CE".equals(t) || "PE".equals(t);
                })
                .mapToLong(NseInstrument::getExpiry)
                .filter(exp -> exp >= now)
                .min();

        if (nearestExpiryOpt.isEmpty()) {
            log.warn("⚠️ No future NIFTY CE/PE expiry found in NSE.json.");
            return;
        }
        long targetExpiry = nearestExpiryOpt.getAsLong();
        log.info("🗓️ Selected nearest expiry epoch={} ({} docs will be matched)", targetExpiry, 
                all.stream().filter(i -> i.getExpiry() == targetExpiry).count());

        // 2) Keep only that expiry
        List<NseInstrument> currentCycle = all.stream()
                .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
                .filter(i -> "NSE_FO".equals(i.getSegment()))
                .filter(i -> {
                    String t = i.getInstrumentType();
                    return "CE".equals(t) || "PE".equals(t);
                })
                .filter(i -> i.getExpiry() == targetExpiry)
                .toList();

        mongoTemplate.dropCollection("nse_instruments");
        mongoTemplate.insert(currentCycle, "nse_instruments");
        log.info("✅ nse_instruments refreshed: {} docs for expiry={}", currentCycle.size(), targetExpiry);
    } catch (Exception e) {
        log.error("❌ Failed refreshing nse_instruments by nearest expiry", e);
    }
}

public void purgeExpiredOptionDocs() {
    long now = System.currentTimeMillis();
    Query expired = new Query(Criteria.where("expiry").lt(now));

    long del1 = mongoTemplate.remove(expired, "nse_instruments").getDeletedCount();
    long del2 = mongoTemplate.remove(expired, "filtered_nifty_premiums").getDeletedCount();
    log.info("🧹 Purged expired: nse_instruments={}, filtered_nifty_premiums={}", del1, del2);
}

private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

public boolean isExpiryCompleted(long expiryMs) {
    LocalDate expiryDay = Instant.ofEpochMilli(expiryMs).atZone(IST).toLocalDate();
    long cutoffMs = expiryDay.atTime(EXPIRY_CUTOFF).atZone(IST).toInstant().toEpochMilli();
    return System.currentTimeMillis() >= cutoffMs;
}

// NEW: choose current‑month FUT key (fallback to nearest)
public Optional<String> getCurrentMonthNiftyFutKey() {
    List<NseInstrument> futs = mongoTemplate.findAll(NseInstrument.class, "nifty_futures");
    if (futs == null || futs.isEmpty()) return Optional.empty();

    ZonedDateTime now = ZonedDateTime.now(IST);
    int y = now.getYear(), m = now.getMonthValue();

    return futs.stream()
            .sorted(Comparator.comparingLong(NseInstrument::getExpiry))
            .filter(f -> {
                LocalDate d = Instant.ofEpochMilli(f.getExpiry()).atZone(IST).toLocalDate();
                return d.getYear() == y && d.getMonthValue() == m;
            })
            .map(NseInstrument::getInstrument_key)
            .findFirst()
            .or(() -> futs.stream()
                    .sorted(Comparator.comparingLong(NseInstrument::getExpiry))
                    .map(NseInstrument::getInstrument_key)
                    .findFirst());
}
// NEW: returns [startMs, endMs] for the nearest future option expiry DATE present in NSE.json (IST)
private long[] nearestOptionExpiryDayWindowFromJsonIST() {
    try {
        File jsonFile = new File("src/main/resources/data/NSE.json");
        List<NseInstrument> all = mapper.readValue(jsonFile, new com.fasterxml.jackson.core.type.TypeReference<>() {});

        long now = System.currentTimeMillis();
        List<LocalDate> days = all.stream()
                .filter(i -> "NSE_FO".equals(i.getSegment()))
                .filter(i -> "NIFTY".equalsIgnoreCase(i.getName()))
                .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
                .filter(i -> {
                    String t = i.getInstrumentType();
                    return "CE".equalsIgnoreCase(t) || "PE".equalsIgnoreCase(t);
                })
                .map(i -> Instant.ofEpochMilli(i.getExpiry()).atZone(IST).toLocalDate())
                .distinct()
                .sorted()
                .toList();

        LocalDate target = days.stream()
                .filter(d -> d.atStartOfDay(IST).toInstant().toEpochMilli() >= now)
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("No future option expiry date in NSE.json"));

        long start = target.atStartOfDay(IST).toInstant().toEpochMilli();
        long end   = target.plusDays(1).atStartOfDay(IST).toInstant().toEpochMilli();
        log.info("🎯 Nearest option expiry date (IST) from JSON: {} ({}..{})", target, start, end);
        return new long[]{start, end};
    } catch (Exception e) {
        throw new RuntimeException("nearestOptionExpiryDayWindowFromJsonIST failed", e);
    }
}
// NEW: compute 10 CE + 10 PE for nearest expiry day and save -> filtered_nifty_premiums
public void filterTenTenAroundLtpAndSave(double niftyLtp) {
    long[] win = nearestOptionExpiryDayWindowFromJsonIST();
    long start = win[0], end = win[1];

    // Try DB universe for this day; fallback to JSON if empty
    var q = new Query(Criteria.where("underlying_key").is("NSE_INDEX|Nifty 50")
            .and("segment").is("NSE_FO")
            .and("instrumentType").in("CE","PE")
            .and("expiry").gte(start).lt(end));
    List<NseInstrument> universe = mongoTemplate.find(q, NseInstrument.class, "nse_instruments");

    if (universe.isEmpty()) {
        try {
            File jsonFile = new File("src/main/resources/data/NSE.json");
            List<NseInstrument> all = mapper.readValue(jsonFile, new com.fasterxml.jackson.core.type.TypeReference<>() {});
            for (NseInstrument i : all) {
                if (i.getName()!=null) i.setName(i.getName().trim());
                if (i.getSegment()!=null) i.setSegment(i.getSegment().trim());
                if (i.getInstrumentType()!=null) i.setInstrumentType(i.getInstrumentType().trim());
                if (i.getUnderlying_key()!=null) i.setUnderlying_key(i.getUnderlying_key().trim());
            }
            universe = all.stream()
                    .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
                    .filter(i -> "NSE_FO".equals(i.getSegment()))
                    .filter(i -> {
                        String t = i.getInstrumentType();
                        return "CE".equals(t) || "PE".equals(t);
                    })
                    .filter(i -> i.getExpiry()>=start && i.getExpiry()<end)
                    .toList();

            mongoTemplate.dropCollection("nse_instruments");
            mongoTemplate.insert(universe, "nse_instruments");
            log.info("ℹ️ Refreshed nse_instruments for nearest expiry day ({} docs)", universe.size());
        } catch (Exception e) {
            log.error("❌ JSON fallback failed", e);
            return;
        }
    }

    int atm = (int) (Math.round(niftyLtp / 50.0) * 50);

    // CE buckets (ITM below ATM, OTM above)
    var ceATM = universe.stream().filter(i -> "CE".equals(i.getInstrumentType()) && i.getStrikePrice()==atm).toList();
    var ceITM = universe.stream().filter(i -> "CE".equals(i.getInstrumentType()) && i.getStrikePrice()<atm)
            .sorted(Comparator.comparingDouble(NseInstrument::getStrikePrice).reversed()).toList();
    var ceOTM = universe.stream().filter(i -> "CE".equals(i.getInstrumentType()) && i.getStrikePrice()>atm)
            .sorted(Comparator.comparingDouble(NseInstrument::getStrikePrice)).toList();

    // PE buckets (ITM above ATM, OTM below)
    var peATM = universe.stream().filter(i -> "PE".equals(i.getInstrumentType()) && i.getStrikePrice()==atm).toList();
    var peITM = universe.stream().filter(i -> "PE".equals(i.getInstrumentType()) && i.getStrikePrice()>atm)
            .sorted(Comparator.comparingDouble(NseInstrument::getStrikePrice)).toList();
    var peOTM = universe.stream().filter(i -> "PE".equals(i.getInstrumentType()) && i.getStrikePrice()<atm)
            .sorted(Comparator.comparingDouble(NseInstrument::getStrikePrice).reversed()).toList();

    List<NseInstrument> pick = new ArrayList<>(20);

    // CE: ATM1 + ITM2 + OTM7
    if (!ceATM.isEmpty()) pick.add(ceATM.get(0));
    pick.addAll(ceITM.stream().limit(2).toList());
    pick.addAll(ceOTM.stream().limit(7).toList());

    // PE: ATM1 + ITM2 + OTM7
    if (!peATM.isEmpty()) pick.add(peATM.get(0));
    pick.addAll(peITM.stream().limit(2).toList());
    pick.addAll(peOTM.stream().limit(7).toList());

    // de‑dupe by instrument_key, keep order
    LinkedHashMap<String,NseInstrument> uniq = new LinkedHashMap<>();
    for (NseInstrument i : pick) {
        if (i.getInstrument_key()!=null) uniq.putIfAbsent(i.getInstrument_key(), i);
    }
    List<NseInstrument> finalSel = new ArrayList<>(uniq.values());

    long ceCount = finalSel.stream().filter(i -> "CE".equals(i.getInstrumentType())).count();
    long peCount = finalSel.stream().filter(i -> "PE".equals(i.getInstrumentType())).count();
    log.info("🎯 Ten+Ten selection: CE={} PE={} (target 10+10), ATM={}, window=[{}..{}]", ceCount, peCount, atm, start, end);

    mongoTemplate.dropCollection("filtered_nifty_premiums");
    mongoTemplate.insert(finalSel, "filtered_nifty_premiums");
    log.info("✅ Stored {} instruments to filtered_nifty_premiums", finalSel.size());
}
private String formatIstDateTime(long epochMs) {
    try {
        return Instant.ofEpochMilli(epochMs).atZone(IST).toLocalDateTime().toString() + " IST";
    } catch (Exception e) {
        return String.valueOf(epochMs);
    }
}
// Ensures nse_instruments has rows for the intended cycle.
// 1) Try strict Wed IST window (Fri→Wed logic)
// 2) If still empty, fall back to "nearest future expiry in NSE.json"
// Returns how many docs are present after the attempt.
public int ensureWeeklyUniverseLoaded() {
    // --- target window by local rule ---
    LocalDate targetWed = resolveCurrentCycleExpiryWednesdayIST();
    long dayStart = istStartOfDayMs(targetWed);
    long dayEnd   = istEndOfDayMs(targetWed);

    // count existing
    Query cntQ = new Query(new Criteria().andOperator(
            Criteria.where("underlying_key").is("NSE_INDEX|Nifty 50"),
            Criteria.where("segment").is("NSE_FO"),
            Criteria.where("instrumentType").in("CE","PE"),
            Criteria.where("expiry").gte(dayStart).lt(dayEnd)
    ));
    long existing = mongoTemplate.count(cntQ, "nse_instruments");
    if (existing > 0) {
        log.info("🧮 nse_instruments already has {} docs for Wed={} ({}..{})",
                existing, targetWed, formatIstDateTime(dayStart), formatIstDateTime(dayEnd));
        return (int)existing;
    }

    log.warn("⚠️ nse_instruments empty for Wed={} — attempting refresh by local rule…", targetWed);
    refreshNiftyOptionsByNearestExpiryFromJson();

    // re-count
    existing = mongoTemplate.count(cntQ, "nse_instruments");
    if (existing > 0) {
        log.info("✅ nse_instruments loaded by local rule: {} docs for Wed={}", existing, targetWed);
        return (int)existing;
    }

    // --- fallback: nearest future expiry present in NSE.json ---
    log.warn("⚠️ Local rule still empty. Falling back to nearest-future-expiry found in NSE.json …");

    try {
        File jsonFile = new File("src/main/resources/data/NSE.json");
        List<NseInstrument> all = mapper.readValue(jsonFile, new com.fasterxml.jackson.core.type.TypeReference<>() {});

        // normalize
        for (NseInstrument i : all) {
            if (i.getName() != null) i.setName(i.getName().trim());
            if (i.getSegment() != null) i.setSegment(i.getSegment().trim());
            if (i.getInstrumentType() != null) i.setInstrumentType(i.getInstrumentType().trim());
            if (i.getUnderlying_key() != null) i.setUnderlying_key(i.getUnderlying_key().trim());
        }

        long now = System.currentTimeMillis();
        OptionalLong nearestFuture = all.stream()
                .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
                .filter(i -> "NSE_FO".equals(i.getSegment()))
                .filter(i -> {
                    String t = i.getInstrumentType();
                    return "CE".equals(t) || "PE".equals(t);
                })
                .mapToLong(NseInstrument::getExpiry)
                .filter(exp -> exp >= now)
                .sorted()
                .findFirst();

        if (nearestFuture.isEmpty()) {
            log.error("❌ NSE.json has no future CE/PE expiry for NIFTY. Cannot build universe.");
            return 0;
        }

        long chosenExpiry = nearestFuture.getAsLong();
        LocalDate chosenDay = Instant.ofEpochMilli(chosenExpiry).atZone(IST).toLocalDate();
        long chosenStart = istStartOfDayMs(chosenDay);
        long chosenEnd   = istEndOfDayMs(chosenDay);
        log.warn("🔁 Fallback selecting expiry day={} ({}..{})",
                chosenDay, formatIstDateTime(chosenStart), formatIstDateTime(chosenEnd));

        List<NseInstrument> current = all.stream()
                .filter(i -> "NSE_INDEX|Nifty 50".equals(i.getUnderlying_key()))
                .filter(i -> "NSE_FO".equals(i.getSegment()))
                .filter(i -> {
                    String t = i.getInstrumentType();
                    return "CE".equals(t) || "PE".equals(t);
                })
                .filter(i -> i.getExpiry() >= chosenStart && i.getExpiry() < chosenEnd)
                .toList();

        if (current.isEmpty()) {
            log.error("❌ Fallback day also yields zero rows. Check NSE.json content.");
            return 0;
        }

        mongoTemplate.dropCollection("nse_instruments");
        mongoTemplate.insert(current, "nse_instruments");
        log.info("💾 nse_instruments populated by fallback: {} docs for day={}", current.size(), chosenDay);

        return current.size();

    } catch (Exception e) {
        log.error("❌ ensureWeeklyUniverseLoaded fallback failed", e);
        return 0;
    }
}
// ==== Epoch + normalize helpers (ADD ONCE) ====
private static boolean isMillis(long v) { return v >= 1_000_000_000_000L; }
private static long normalizeEpoch(long raw) { return raw <= 0 ? 0L : (isMillis(raw) ? raw : raw * 1000L); }

private void normalizeInstrument(NseInstrument i) {
    if (i.getName() != null) i.setName(i.getName().trim());
    if (i.getSegment() != null) i.setSegment(i.getSegment().trim());
    if (i.getInstrumentType() != null) i.setInstrumentType(i.getInstrumentType().trim());
    if (i.getUnderlying_key() != null) i.setUnderlying_key(i.getUnderlying_key().trim());
    // normalize expiry to ms
    i.setExpiry(normalizeEpoch(i.getExpiry()));
}
}
