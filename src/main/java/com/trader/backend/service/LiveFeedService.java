package com.trader.backend.service;



import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.util.JsonFormat;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import com.trader.backend.entity.NseInstrument;
import com.upstox.marketdatafeederv3udapi.rpc.proto.MarketDataFeed;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.time.Duration;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.beans.factory.annotation.Value;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Map;
import org.springframework.context.event.EventListener;
import com.trader.backend.events.FilteredPremiumsUpdatedEvent;
import com.trader.backend.events.LtpEvent;


@Service
@Slf4j
@RequiredArgsConstructor
public class LiveFeedService {

    @Autowired(required = false)
    private WriteApiBlocking writeApi;

    private final UpstoxAuthService auth;
    private final NseInstrumentService nseInstrumentService;
    private final ObjectMapper om = new ObjectMapper();
    private final MongoTemplate mongoTemplate;
    private final QuantAnalysisService quantAnalysisService;
    @Value("${app.mock:false}")
    private boolean mockMode;
// Tracks currently subscribed CE/PE instruments for debug/monitoring
private final Set<String> subscribed = ConcurrentHashMap.newKeySet();
private final Sinks.Many<JsonNode> sink = Sinks.many().multicast().onBackpressureBuffer();
private final AtomicBoolean optionsStreamStarted = new AtomicBoolean(false);
private final AtomicBoolean started = new AtomicBoolean(false);

    private final Sinks.Many<LtpEvent> ltpSink = Sinks.many().multicast().onBackpressureBuffer();
    public Flux<LtpEvent> ltpEvents() { return ltpSink.asFlux(); }

    @Value("${ltp.log.enabled:true}")
    private boolean ltpLogEnabled;
    @Value("${ltp.log.interval.ms:2000}")
    private long ltpLogIntervalMs;
    @Value("${ltp.log.min.price.delta:0.5}")
    private double ltpLogMinDelta;
    @Value("${influx.summary.interval.ms:15000}")
    private long influxSummaryMs;

    private final Map<String, LogState> logState = new ConcurrentHashMap<>();
    private static class LogState { long lastTs; double lastPrice; }

    private final Map<String, NseInstrument> instrumentCache = new ConcurrentHashMap<>();
    private final Set<String> loggedInstruments = ConcurrentHashMap.newKeySet();
    private final AtomicLong futWrites = new AtomicLong();
    private final AtomicLong optWrites = new AtomicLong();
    private final AtomicLong futWriteFails = new AtomicLong();
    private final AtomicLong optWriteFails = new AtomicLong();
    private final AtomicReference<String> lastInfluxError = new AtomicReference<>("");
    /**
     * Exposed for your controllers to subscribe
     **/
    public Flux<JsonNode> stream() {
        return sink.asFlux();
    }
    @PostConstruct
    public void subscribeToAuthEvents() {
        auth.events()
                .filter(e -> e == UpstoxAuthService.AuthEvent.READY)
                .take(1)
                .subscribe(ev -> initLiveWebSocket());

        auth.events()
                .filter(e -> e == UpstoxAuthService.AuthEvent.EXPIRED)
                .subscribe(e -> {
                    log.warn("Auth token expired or refresh failed; waiting for re-login");
                    started.set(false);
                });

        Flux.interval(Duration.ofMillis(influxSummaryMs))
                .subscribe(i -> {
                    long fut = futWrites.getAndSet(0);
                    long opt = optWrites.getAndSet(0);
                    long futFail = futWriteFails.getAndSet(0);
                    long optFail = optWriteFails.getAndSet(0);
                    log.info("Influx summary — FUT writes={}, OPT writes={} (last {}s)", fut, opt, influxSummaryMs / 1000);
                    if (futFail > 0 || optFail > 0) {
                        log.warn("Influx failures — FUT={} OPT={} lastError={}", futFail, optFail, lastInfluxError.get());
                        lastInfluxError.set("");
                    }
                });
    }

    public void initLiveWebSocket() {
        if (mockMode) {
            log.info("🧪 Mock mode enabled - skipping live feed startup");
            return;
        }
        if (started.compareAndSet(false, true)) {
            log.info("OAuth success — starting live streaming…");
            try {
                nseInstrumentService.purgeExpiredOptionDocs();
                nseInstrumentService.refreshNiftyOptionsByNearestExpiryFromJson();
                nseInstrumentService.saveNiftyFuturesToMongo();
                streamNiftyFutAndTriggerCEPE();
            } catch (Exception e) {
                log.error("❌ init sequence failed", e);
            }
        }
    }

    /**
     * STEP 6.1: fetch the actual WS URL (handles redirect or JSON token)
     **/
    public Mono<String> fetchWebSocketUrl() {
        log.info("⟳ entering fetchWebSocketUrl(), current token={}", auth.currentToken());
        return WebClient.builder()
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + auth.currentToken())
                .build()
                .get()
                .uri("https://api.upstox.com/v3/feed/market-data-feed/authorize")
                .exchangeToMono(resp -> {
                    // 1) if they give a redirect, just grab it
                    if (resp.statusCode().is3xxRedirection()) {
                        String redirect = resp.headers()
                                .asHttpHeaders()
                                .getLocation()
                                .toString();
                        return Mono.just(redirect);
                    }


                    return resp.bodyToMono(JsonNode.class)
                            .flatMap(j -> {
                                log.debug("→ /authorize JSON payload: {}", j);
                                // pull out the wss:// URI directly
                                JsonNode data = j.path("data");
                                String wsUrl = data.has("authorizedRedirectUri")
                                        ? data.get("authorizedRedirectUri").asText()
                                        : data.get("authorized_redirect_uri").asText();
                                log.info("▶︎ connecting to WS at {}", wsUrl);
                                return Mono.just(wsUrl);
                            });
                });
    }


    private static final byte[] SUB_FRAME = """
            {"guid":"someguid","method":"sub",
             "data":{"mode":"full",
                     "instrumentKeys":["NSE_FO|44874"]}}
            """.getBytes(StandardCharsets.UTF_8);

    private Flux<JsonNode> openWebSocket(String wsUrl) {
        ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();
        Sinks.Many<JsonNode> local = Sinks.many().multicast().onBackpressureBuffer();

        client.execute(URI.create(wsUrl), session ->
                // 1) send our JSON-as-binary SUB_FRAME
                session.send(Mono.just(session.binaryMessage(bb -> bb.wrap(SUB_FRAME))))
                        .doOnSuccess(v -> log.info("▶︎ subscribe frame sent"))
                        // 2) then receive raw protobuf frames and parse them
                        .thenMany(session.receive()
                                .map(WebSocketMessage::getPayload)           // DataBuffer
                                .map(this::parseProtoFeedResponse)           // FeedResponse → JsonNode
                                .doOnNext(local::tryEmitNext)
                        )
                        .then()
        ).subscribe();

        return local.asFlux();
    }

    /**
     * replace your old parseProtoTick with this:
     **/
    private JsonNode parseProtoFeedResponse(DataBuffer buf) {
        try {
            byte[] b = new byte[buf.readableByteCount()];
            buf.read(b);

            // Upstox sample parses FeedResponse
            var resp = MarketDataFeed.FeedResponse.parseFrom(b);

            // convert to JSON with protobuf’s JsonFormat
            String json = JsonFormat.printer()
                    .omittingInsignificantWhitespace()
                    .print(resp);

            return om.readTree(json);
        } catch (Exception ex) {
            throw Exceptions.propagate(ex);
        }
    }

    private Point toPoint(JsonNode tick) {
        // 1) discover the instrument key (first field under "feeds")
        JsonNode feeds = tick.path("feeds");
        Iterator<String> it = feeds.fieldNames();
        String instr = it.hasNext() ? it.next() : "UNKNOWN";

        // 2) pick timestamp: currentTs if present, else now()
        long tms = tick.hasNonNull("currentTs")
                ? tick.get("currentTs").asLong()
                : Instant.now().toEpochMilli();

        // 3) build your InfluxDB point, saving the whole JSON as a string
        return Point
                .measurement("ticks")
                .addTag("instrument", instr)
                .time(Instant.ofEpochMilli(tms), WritePrecision.MS)
                .addField("raw", tick.toString());
    }
    public void setupNiftyOptionsLiveFeed() {
        log.info("🚀 Starting Nifty Option Chain setup...");
        log.info("🚀 [INIT] setupNiftyOptionsLiveFeed() CALLED");

        WebClient.builder()
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + auth.currentToken())
                .build()
                .get()
                .uri("https://api.upstox.com/v2/option/chain/index/NIFTY")
                .retrieve()
                .bodyToMono(JsonNode.class)
                .flatMapMany(response -> {
                    log.debug("🔍 Full response from option chain: {}", response.toPrettyString());

                    JsonNode records = response.path("data").path("records");
                    if (records.isMissingNode() || !records.isArray()) {
                        log.error("⚠️ Option chain response invalid or empty");
                        return Flux.empty();
                    }

                    String nearestExpiry = "";
                    List<String> instrumentKeys = new ArrayList<>();
                    DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyMMMdd").withLocale(Locale.ENGLISH);
                    String segment = "NSE_INDEX_OPT";
                    String symbol = "NIFTY";

                    for (JsonNode record : records) {
                        String expiry = record.path("expiryDate").asText(); // yyyy-MM-dd
                        if (nearestExpiry.isEmpty()) {
                            nearestExpiry = expiry;
                            log.info("📅 Nearest expiry detected: {}", nearestExpiry);
                        }
                        if (!expiry.equals(nearestExpiry)) continue;

                        LocalDate expDate = LocalDate.parse(expiry);
                        String formattedExpiry = expDate.format(fmt).toUpperCase();

                        JsonNode ce = record.path("CE");
                        JsonNode pe = record.path("PE");

                        if (ce != null && ce.has("lastPrice") && ce.has("strikePrice")) {
                            double ceLtp = ce.path("lastPrice").asDouble();
                            double strike = ce.path("strikePrice").asDouble();
                            if (ceLtp < 50) {
                                String key = String.format("%s|%s%s%sCE", segment, symbol, formattedExpiry, (int) strike);
                                log.info("📘 CE → LTP: {}, Key: {}", ceLtp, key);
                                instrumentKeys.add(key);
                            }
                        }

                        if (pe != null && pe.has("lastPrice") && pe.has("strikePrice")) {
                            double peLtp = pe.path("lastPrice").asDouble();
                            double strike = pe.path("strikePrice").asDouble();
                            if (peLtp < 50) {
                                String key = String.format("%s|%s%s%sPE", segment, symbol, formattedExpiry, (int) strike);
                                log.info("📕 PE → LTP: {}, Key: {}", peLtp, key);
                                instrumentKeys.add(key);
                            }
                        }
                    }

                    log.info("✅ Total filtered option keys: {}", instrumentKeys.size());
                    for (String key : instrumentKeys) {
                        log.info("📦 Subscribing to option key: {}", key);
                    }

                    if (instrumentKeys.isEmpty()) {
                        log.warn("⚠️ No options found under ₹50. Nothing to subscribe.");
                        return Flux.empty();
                    }

                    ObjectMapper localMapper = new ObjectMapper();
                    ObjectNode frame = localMapper.createObjectNode();
                    frame.put("guid", "nifty-options-guid");
                    frame.put("method", "sub");

                    ObjectNode data = frame.putObject("data");
                    data.put("mode", "full");
                    ArrayNode keysArray = data.putArray("instrumentKeys");
                    for (String key : instrumentKeys) {
                        keysArray.add(key);
                    }

                    byte[] frameBytes;
                    try {
                        frameBytes = frame.toString().getBytes(StandardCharsets.UTF_8);
                        log.debug("🧾 Final SUB_FRAME for Nifty Options: {}", frame.toPrettyString());
                    } catch (Exception e) {
                        log.error("❌ Failed to build SUB_FRAME", e);
                        return Flux.empty();
                    }

                    return fetchWebSocketUrl()
                            .flatMapMany(wsUrl -> openWebSocketForOptions(wsUrl, frameBytes));
                })
                .subscribe(
                        tick -> sink.tryEmitNext((JsonNode) tick),
                        error -> log.error("❌ Option WS failed: ", error)
                );
    }


    public Flux<JsonNode> openWebSocketForOptions(String wsUrl, byte[] subFrame) {
        ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();
        Sinks.Many<JsonNode> local = Sinks.many().multicast().onBackpressureBuffer();

        client.execute(URI.create(wsUrl), session ->
                session.send(Mono.just(session.binaryMessage(bb -> bb.wrap(subFrame))))
                        .doOnSuccess(v -> log.info("▶︎ Nifty options subscription frame sent"))
                        .thenMany(session.receive()
                                .map(WebSocketMessage::getPayload)
                                .map(this::parseProtoFeedResponse)
                                .doOnNext(local::tryEmitNext)
                                .doOnSubscribe(s -> log.info("📡 Subscribed to Nifty options WebSocket feed"))

                        )
                        .then()
        ).subscribe();

        return local.asFlux();
    }
    // inside LiveFeedService.java (below your existing SUB_FRAME):
    private static final byte[] OPTION_SUB_FRAME = """
    {
      "guid":"someguid‐options",
      "method":"sub",
      "data":{
        "mode":"full",
        "instrumentKeys":[
          "NSE_FO|60131"
        ]
      }
    }
    """.getBytes(StandardCharsets.UTF_8);
    private Flux<JsonNode> openOptionWebSocket(String wsUrl) {
        ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();
        Sinks.Many<JsonNode> local = Sinks.many().multicast().onBackpressureBuffer();

        client.execute(URI.create(wsUrl), session ->
                // 1) send OPTION_SUB_FRAME instead of SUB_FRAME
                session.send(Mono.just(session.binaryMessage(bb -> bb.wrap(OPTION_SUB_FRAME))))
                        .doOnSuccess(v -> log.info("▶︎ option‐subscribe frame sent"))
                        // 2) receive protobuf → JsonNode
                        .thenMany(session.receive()
                                .map(WebSocketMessage::getPayload)
                                .map(this::parseProtoFeedResponse)
                                .doOnNext(local::tryEmitNext)
                        )
                        .then()
        ).subscribe();

        return local.asFlux();
    }
    public void streamFilteredNiftyOptions() {
    // Prevent duplicate WS streams for the same filtered list
    if (!optionsStreamStarted.compareAndSet(false, true)) {
        log.info("🔁 filtered CE/PE stream already running; keeping existing connection.");
        return;
    }

    // Sanity check: do we have anything to subscribe to right now?
    var initialKeys = nseInstrumentService.getInstrumentKeysForLiveSubscription();
    if (initialKeys.isEmpty()) {
        optionsStreamStarted.set(false);
        log.warn("⚠️ filtered_nifty_premiums is empty — skipping option stream start for now.");
        return;
    }
    log.info("🚀 (re)starting live stream for filtered CE/PE — {} keys found in Mongo.", initialKeys.size());

    auth.ensureValidToken()
        .flatMap(valid -> {
            if (!valid) {
                optionsStreamStarted.set(false);
                log.warn("⚠️ Upstox token not ready — skipping option stream start");
                return Mono.empty();
            }
            return fetchWebSocketUrl();
        })
        // buildFilteredSubFrame pulls the latest keys on every (re)connect
        .flatMapMany(wsUrl -> openWebSocketWithDynamicSub(wsUrl, this::buildFilteredSubFrame))
        .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(5)))
        .doOnSubscribe(s -> log.info("📡 Subscribed to filtered CE/PE (auto-resub on reconnect)"))
        .doOnNext(tick -> {
            sink.tryEmitNext(tick);
            JsonNode feeds = tick.path("feeds");
            feeds.fields().forEachRemaining(entry -> {
                String instrumentKey = entry.getKey();
                JsonNode feed = entry.getValue();
                JsonNode ltpNode = feed
                        .path("fullFeed")
                        .path("marketFF")
                        .path("ltpc")
                        .path("ltp");
                if (ltpNode.isNumber()) {
                    double ltp = ltpNode.asDouble();
                    long ts = extractTimestamp(feed, tick);
                    ltpSink.tryEmitNext(new LtpEvent(instrumentKey, ltp, Instant.ofEpochMilli(ts)));
                    maybeLogLtp(instrumentKey, ltp, ts);
                    writeTickToInflux(instrumentKey, feed, ts);

                    var result = quantAnalysisService.analyze(instrumentKey, feed);
                    if (result.signal() != QuantAnalysisService.Signal.NONE) {
                        log.info("🎯 {} for {} → momentum={} volSpike={} imbalance={} noise={}",
                                result.signal(), instrumentKey,
                                String.format("%.4f", result.momentum()),
                                String.format("%.2f", result.volumeSpike()),
                                String.format("%.4f", result.imbalance()),
                                String.format("%.2f", result.noise()));
                    }
                }
            });
        })
        .doOnError(err -> {
            optionsStreamStarted.set(false); // allow a fresh start after a fatal error
            log.error("❌ filtered option feed failed:", err);
        })
        .doFinally(sig -> {
            // If the stream ever completes/cancels, allow a fresh start later
            optionsStreamStarted.set(false);
            log.info("🧹 filtered CE/PE stream terminated: {}", sig);
        })
        .subscribe();
    }
public void streamSingleInstrument(String instrumentKey) {
    log.info("🚀 Starting live stream for instrument → {}", instrumentKey);

    // Step 1: Create a sub frame
    ObjectNode frame = om.createObjectNode();
    frame.put("guid", "single-instrument-guid");
    frame.put("method", "sub");

    ObjectNode data = frame.putObject("data");
    data.put("mode", "full");
    data.putArray("instrumentKeys").add(instrumentKey);

    byte[] subFrame = frame.toString().getBytes(StandardCharsets.UTF_8);

    // Step 2: Connect and stream
    fetchWebSocketUrl()
        .flatMapMany(wsUrl -> openWebSocketForOptions(wsUrl, subFrame))
        .doOnNext(tick -> {
            try {
                // ✅ Parse LTP from incoming tick
                // ✅ Correct path:
JsonNode ltpNode = tick.path("feeds")
    .path(instrumentKey)
    .path("fullFeed")
    .path("marketFF")
    .path("ltpc")
    .path("ltp");
                if (!ltpNode.isMissingNode()) {
                    double ltp = ltpNode.asDouble();
                    long ts = extractTimestamp(tick.path("feeds").path(instrumentKey), tick);
                    maybeLogLtp(instrumentKey, ltp, ts);
                    ltpSink.tryEmitNext(new LtpEvent(instrumentKey, ltp, Instant.ofEpochMilli(ts)));
                    writeTickToInflux(instrumentKey, tick.path("feeds").path(instrumentKey), ts);
                    nseInstrumentService.filterStrikesAroundLtp(ltp);
                }

                sink.tryEmitNext(tick);
            } catch (Exception e) {
                log.error("❌ Error parsing tick JSON or filtering: ", e);
            }
        })
        .doOnError(err -> log.error("❌ WebSocket stream failed:", err))
        .subscribe();
}

public byte[] buildSubFrame(String instrumentKey) {
    ObjectNode frame = om.createObjectNode();
    frame.put("guid", "nifty-fut-guid");
    frame.put("method", "sub");

    ObjectNode data = frame.putObject("data");
    data.put("mode", "full");
    data.putArray("instrumentKeys").add(instrumentKey);

    return frame.toString().getBytes(StandardCharsets.UTF_8);
}
public MongoTemplate getMongoTemplate() {
    return mongoTemplate;
}
public void streamNiftyFutAndTriggerCEPE() {
    log.info("🚀 Subscribing to NIFTY FUT to extract LTP and filter CE/PE...");

    if (auth.currentToken() == null) {
        log.warn("⚠️ Cannot start NIFTY FUT stream — token not available");
        return;
    }

    Optional<String> optKey = nseInstrumentService.nearestNiftyFutureKey();
    if (optKey.isEmpty()) {
        log.error("❌ No valid NIFTY FUT contract to subscribe.");
        return;
    }
    String instrumentKey = optKey.get();
    log.info("📦 Subscribing to NIFTY FUT: {}", instrumentKey);

    fetchWebSocketUrl()
            .flatMapMany(wsUrl -> openWebSocketForOptions(wsUrl, buildSubFrame(instrumentKey)))
            .doOnNext(tick -> {
                try {
                    JsonNode feed = tick.path("feeds").path(instrumentKey);
                    JsonNode ltpNode = feed
                            .path("fullFeed")
                            .path("marketFF")
                            .path("ltpc")
                            .path("ltp");

                    if (ltpNode.isNumber()) {
                        double ltp = ltpNode.asDouble();
                        long ts = extractTimestamp(feed, tick);
                        ltpSink.tryEmitNext(new LtpEvent(instrumentKey, ltp, Instant.ofEpochMilli(ts)));
                        maybeLogLtp(instrumentKey, ltp, ts);
                        writeTickToInflux(instrumentKey, feed, ts);

                        nseInstrumentService.filterStrikesAroundLtp(ltp);
                        streamFilteredNiftyOptions();
                    } else {
                        log.warn("⚠️ LTP not found in tick — instrumentKey={}", instrumentKey);
                    }
                } catch (Exception ex) {
                    log.error("⚠️ Failed to extract LTP or trigger filtering", ex);
                }
            })
            .doOnError(err -> log.error("❌ WebSocket stream failed:", err))
            .subscribe();
}
/** Builds a fresh SUB frame from the current filtered_nifty_premiums (15 CE + 15 PE). */
private byte[] buildFilteredSubFrame() {
    List<String> keys = nseInstrumentService.getInstrumentKeysForLiveSubscription();
    if (keys.isEmpty()) {
        log.warn("⚠️ No filtered instruments found in Mongo (filtered_nifty_premiums).");
    } else {
        // optional: track what we're subscribing to (debug)
        subscribed.clear();
        subscribed.addAll(keys);
        log.info("📦 Will (re)subscribe {} instruments from filtered_nifty_premiums", keys.size());
    }

    ObjectNode frame = om.createObjectNode();
    frame.put("guid", "filtered-options-guid");
    frame.put("method", "sub");

    ObjectNode data = frame.putObject("data");
    data.put("mode", "full");
    ArrayNode arr = data.putArray("instrumentKeys");
    keys.forEach(arr::add);

    return frame.toString().getBytes(StandardCharsets.UTF_8);
}
/**
 * Opens a WS and sends a fresh SUB frame per connection (frameSupplier is called on every connect).
 * Use this for dynamic lists that may change between reconnects.
 */
private Flux<JsonNode> openWebSocketWithDynamicSub(String wsUrl, java.util.function.Supplier<byte[]> frameSupplier) {
    ReactorNettyWebSocketClient client = new ReactorNettyWebSocketClient();
    Sinks.Many<JsonNode> local = Sinks.many().multicast().onBackpressureBuffer();

    client.execute(URI.create(wsUrl), session ->
            session.send(Mono.just(session.binaryMessage(bb -> bb.wrap(frameSupplier.get()))))
                   .doOnSuccess(v -> log.info("▶︎ dynamic subscribe frame sent"))
                   .thenMany(session.receive()
                           .map(WebSocketMessage::getPayload)
                           .map(this::parseProtoFeedResponse)
                           .doOnNext(local::tryEmitNext))
                   .then()
    ).subscribe();

    return local.asFlux();
}

    private void maybeLogLtp(String instrumentKey, double ltp, long ts) {
        if (!ltpLogEnabled) return;
        LogState st = logState.computeIfAbsent(instrumentKey, k -> new LogState());
        if (ts - st.lastTs >= ltpLogIntervalMs && Math.abs(ltp - st.lastPrice) >= ltpLogMinDelta) {
            log.info("LTP key={} price={} ts={}", instrumentKey, ltp, Instant.ofEpochMilli(ts));
            st.lastTs = ts;
            st.lastPrice = ltp;
        }
    }

    private void writeTickToInflux(String instrumentKey, JsonNode feed, long ts) {
        if (writeApi == null) return;
        NseInstrument info = instrumentCache.computeIfAbsent(instrumentKey,
                k -> mongoTemplate.findById(k, NseInstrument.class));
        if (info == null) return;
        boolean isFut = info.getInstrumentType() != null && info.getInstrumentType().toUpperCase().contains("FUT");
        String measurement = isFut ? "nifty_fut_ticks" : "nifty_option_ticks";

        long exp = info.getExpiry();
        if (exp < 1_000_000_000_000L) exp *= 1000L;
        String expiry = Instant.ofEpochMilli(exp).toString().substring(0, 10);
        String symbol = info.getTrading_symbol() != null ? info.getTrading_symbol() : info.getName();

        Point p = Point.measurement(measurement)
                .addTag("instrumentKey", instrumentKey)
                .addTag("symbol", symbol == null ? "" : symbol)
                .addTag("segment", info.getSegment())
                .addTag("type", info.getInstrumentType())
                .addTag("expiry", expiry);
        String t = info.getInstrumentType();
        if (!isFut && t != null && (t.equalsIgnoreCase("CE") || t.equalsIgnoreCase("PE"))) {
            p.addTag("side", t.toUpperCase());
        }

        addNumericFields(p, feed, "");
        p.addField("raw", feed.toString());
        p.time(Instant.ofEpochMilli(ts), WritePrecision.MS);

        try {
            writeApi.writePoint(p);
            if (loggedInstruments.add(instrumentKey)) {
                log.info("Influx write OK — measurement={} key={}", measurement, instrumentKey);
            }
            if (isFut) futWrites.incrementAndGet(); else optWrites.incrementAndGet();
        } catch (Exception e) {
            lastInfluxError.set(e.getMessage());
            if (isFut) futWriteFails.incrementAndGet(); else optWriteFails.incrementAndGet();
        }
    }

    private void addNumericFields(Point p, JsonNode node, String prefix) {
        node.fields().forEachRemaining(entry -> {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + '_' + entry.getKey();
            JsonNode val = entry.getValue();
            if (val.isNumber()) {
                p.addField(key, val.asDouble());
            } else if (val.isObject()) {
                addNumericFields(p, val, key);
            }
        });
    }

    private long extractTimestamp(JsonNode feed, JsonNode tick) {
        String[] ptrs = {"/fullFeed/marketFF/ts", "/fullFeed/ts", "/ts", "/timestamp"};
        for (String ptr : ptrs) {
            JsonNode n = feed.at(ptr);
            if (n.isNumber()) {
                long v = n.asLong();
                return v < 1_000_000_000_000L ? v * 1000L : v;
            }
        }
        JsonNode c = tick.path("currentTs");
        if (c.isNumber()) {
            long v = c.asLong();
            return v < 1_000_000_000_000L ? v * 1000L : v;
        }
        return System.currentTimeMillis();
    }


@EventListener
public void onFilteredPremiumsUpdated(FilteredPremiumsUpdatedEvent e) {
    log.info("📬 Filtered premiums updated: {} docs for expiry={} → (re)subscribing…",
            e.getCount(), e.getExpiryEpochMs());
    streamFilteredNiftyOptions();  // this already builds the sub-frame from Mongo and subscribes
}
}
