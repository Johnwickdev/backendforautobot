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
import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.event.EventListener;
import com.trader.backend.events.FilteredPremiumsUpdatedEvent;


@Service
@Slf4j
@RequiredArgsConstructor
public class LiveFeedService {
    private final WriteApiBlocking writeApi ;
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
    /**
     * Exposed for your controllers to subscribe
     **/
    public Flux<JsonNode> stream() {
        return sink.asFlux();
    }


  @PostConstruct
public void initAutoStart() {
    if (mockMode) {
        log.info("🧪 Mock mode enabled - skipping live feed startup");
        // streamNiftyFutAndTriggerCEPE(); // Nifty Future live ticks
        // setupNiftyOptionsLiveFeed();   // Option chain live ticks
        return;
    }
    log.info("🚀 Starting auto init sequence...");

    // make sure token is valid before any network calls
    auth.ensureValidToken()
        .doOnSuccess(v -> {
            try {
                // 1) clean up old data
                nseInstrumentService.purgeExpiredOptionDocs();

                // 2) load only CURRENT-WEEK CE/PE into nse_instruments (IST day window)
                nseInstrumentService.refreshNiftyOptionsByNearestExpiryFromJson();

                // 3) save all NIFTY FUTURES (your existing method)
                nseInstrumentService.saveNiftyFuturesToMongo();

                // 4) start FUT stream → first LTP → filterStrikesAroundLtp() → save 15+15 → subscribe CE/PE
                streamNiftyFutAndTriggerCEPE();
            } catch (Exception e) {
                log.error("❌ init sequence failed", e);
            }
        })
        .doOnError(e -> log.error("❌ ensureValidToken failed at startup", e))
        .subscribe();
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
                                .doOnNext(tick -> log.info("⏳ tick → {}", tick))
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
                    log.info("🔍 Full response from option chain: {}", response.toPrettyString());

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
                        log.info("🧾 Final SUB_FRAME for Nifty Options: {}", frame.toPrettyString());
                    } catch (Exception e) {
                        log.error("❌ Failed to build SUB_FRAME", e);
                        return Flux.empty();
                    }

                    return fetchWebSocketUrl()
                            .flatMapMany(wsUrl -> openWebSocketForOptions(wsUrl, frameBytes));
                })
                .subscribe(
                        tick -> {
                            // log.debug("💥 Nifty Option Tick: {}", tick.toPrettyString());
                            sink.tryEmitNext((JsonNode) tick);
                        },
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
                                // log every tick under “ticks for option → …”
                                .doOnNext(optJson -> log.info("⏳ ticks for option → {}", optJson))
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

    Mono.defer(() -> auth.ensureValidToken().then(fetchWebSocketUrl()))
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
                if (!ltpNode.isMissingNode()) {
                    double ltp = ltpNode.asDouble();
                    log.info("📈 Option tick: {} LTP={}", instrumentKey, ltp);

                    var result = quantAnalysisService.analyze(instrumentKey, feed);
                    if (result.signal() != QuantAnalysisService.Signal.NONE) {
                        log.info("🎯 {} for {} → momentum={} volSpike={} imbalance={} noise={}",
                                result.signal(), instrumentKey,
                                String.format("%.4f", result.momentum()),
                                String.format("%.2f", result.volumeSpike()),
                                String.format("%.4f", result.imbalance()),
                                String.format("%.2f", result.noise()));

                        if (result.signal() == QuantAnalysisService.Signal.ENTRY_LONG
                                || result.signal() == QuantAnalysisService.Signal.ENTRY_SHORT) {
                            log.info("⏱ entry noted for {} at {} — scheduling exit in 5s", instrumentKey, ltp);
                            Mono.delay(Duration.ofSeconds(5))
                                    .subscribe(v -> log.info("⏳ exit triggered for {}", instrumentKey));
                        }
                    }
                } else {
                    log.debug("⏳ tick for {}", instrumentKey);
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
                    log.info("📉 LIVE LTP for NIFTY FUT: {}", ltp);

                    // ✅ Trigger CE/PE filtering based on this LTP
                    nseInstrumentService.filterStrikesAroundLtp(ltp);
                } else {
                    log.warn("⚠️ LTP not found in tick");
                }

                // Still emit tick to sink if needed
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

    try {
        File file = new File("src/main/resources/data/NSE.json");
        NseInstrument[] instruments = om.readValue(file, NseInstrument[].class);

        NseInstrument nearestFut = Arrays.stream(instruments)
                .filter(i -> "FUT".equalsIgnoreCase(i.getInstrumentType()))
                .filter(i -> "NIFTY".equalsIgnoreCase(i.getName()))
                .filter(i -> "NSE_FO".equalsIgnoreCase(i.getSegment()))
                .filter(i -> i.getUnderlying_key().equals("NSE_INDEX|Nifty 50"))
                .sorted(Comparator.comparing(NseInstrument::getExpiry))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("No NIFTY FUT found."));

        String instrumentKey = nearestFut.getInstrument_key();
        log.info("📦 Subscribing to NIFTY FUT: {}", instrumentKey);

        fetchWebSocketUrl()
                .flatMapMany(wsUrl -> openWebSocketForOptions(wsUrl, buildSubFrame(instrumentKey)))
                .doOnNext(tick -> {
                    try {
                        // ✅ Correct LTP path under fullFeed → marketFF → ltpc → ltp
                        JsonNode ltpNode = tick.path("feeds")
                                .path(instrumentKey)
                                .path("fullFeed")
                                .path("marketFF")
                                .path("ltpc")
                                .path("ltp");

                        if (ltpNode != null && ltpNode.isNumber()) {
                            double ltp = ltpNode.asDouble();
                            log.info("📈 Extracted NIFTY FUT LTP: {}", ltp);

                            // write to Influx (optional)
                            writeNiftyFutLtpToInflux(ltp, System.currentTimeMillis());

                            // 🔥 Filter & save CE/PE based on LTP
                            nseInstrumentService.filterStrikesAroundLtp(ltp);

                            // 🎯 (Re)start the filtered CE/PE stream (dynamic + reconnect-safe)
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

    } catch (Exception e) {
        log.error("❌ Failed to load NIFTY FUT from file", e);
    }
}
public void writeNiftyFutLtpToInflux(double ltp, long timestamp) {
    Point point = Point
            .measurement("nifty_fut_ltp")
            .addTag("symbol", "NIFTY")
            .addField("ltp", ltp)
            .time(Instant.ofEpochMilli(timestamp), WritePrecision.MS);

    writeApi.writePoint(point);
    log.info("✅ [Influx] NIFTY FUT LTP written: {}", point);
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


@EventListener
public void onFilteredPremiumsUpdated(FilteredPremiumsUpdatedEvent e) {
    log.info("📬 Filtered premiums updated: {} docs for expiry={} → (re)subscribing…",
            e.getCount(), e.getExpiryEpochMs());
    streamFilteredNiftyOptions();  // this already builds the sub-frame from Mongo and subscribes
}
}
