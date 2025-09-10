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
import reactor.netty.http.client.HttpClient;
import io.netty.channel.ChannelOption;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.time.Duration;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpHeaders;
import org.springframework.beans.factory.annotation.Value;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.Map;
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
private final Sinks.Many<JsonNode> sink = Sinks.many().multicast().onBackpressureBuffer();
private final AtomicBoolean optionsStreamStarted = new AtomicBoolean(false);

    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicBoolean everConnected = new AtomicBoolean(false);
    private final AtomicBoolean futSubscribed = new AtomicBoolean(false);
    private final AtomicBoolean futLtpInitialized = new AtomicBoolean(false);
    private final AtomicInteger optSubscribedCount = new AtomicInteger(0);
    private final AtomicLong ticksLast60s = new AtomicLong();
    private final AtomicReference<Instant> lastTickTs = new AtomicReference<>(null);

    // guard against parallel WS connects
    private final AtomicBoolean wsConnecting = new AtomicBoolean(false);
    // remember current FUT instrument for resubscription
    private final AtomicReference<String> futInstrumentKey = new AtomicReference<>(null);

private final AtomicLong lastAutoStartLog = new AtomicLong(0);

    private final AtomicBoolean marketWasOpen = new AtomicBoolean(false);

public enum OrchestratorState { IDLE, RUNNING, READY }
private final AtomicReference<OrchestratorState> orchestratorState = new AtomicReference<>(OrchestratorState.IDLE);
private final AtomicReference<String> lastSelectionSignature = new AtomicReference<>(null);
private final AtomicBoolean selectionComputed = new AtomicBoolean(false);
private final Set<String> currentlySubscribedKeys = ConcurrentHashMap.newKeySet();

    private final ConcurrentHashMap<String, Tick> lastTick = new ConcurrentHashMap<>();
    private final AtomicBoolean instrumentsInitialized = new AtomicBoolean(false);
    private final AtomicInteger ceLoadedCount = new AtomicInteger(0);
    private final AtomicInteger peLoadedCount = new AtomicInteger(0);
    private final AtomicLong currentExpiryMs = new AtomicLong(0);

    public Optional<Tick> getLatestTick(String key) {
        return Optional.ofNullable(lastTick.get(key));
    }

    public Double getLatestLtp(String key) {
        return getLatestTick(key).map(Tick::ltp).orElse(null);
    }

    public Set<String> cachedKeys() {
        return lastTick.keySet();
    }

    public boolean isMarketOpen() {
        return MarketHours.isOpen(Instant.now());
    }

    public boolean hasRecentFutWrites() {
        return futWrites.get() > 0;
    }

    public boolean isConnected() { return connected.get(); }
    public Instant lastTickTs() { return lastTickTs.get(); }
    public long ticksLast60s() { return ticksLast60s.get(); }
    public boolean futSubscribed() { return futSubscribed.get(); }
    public int optSubscribedCount() { return optSubscribedCount.get(); }

    private final Sinks.Many<LtpEvent> ltpSink = Sinks.many().multicast().onBackpressureBuffer();
    public Flux<LtpEvent> ltpEvents() { return ltpSink.asFlux(); }

    @Value("${influx.bucket:}")
    private String influxBucket;
    @Value("${influx.org:}")
    private String influxOrg;

    private final Map<String, NseInstrument> instrumentCache = new ConcurrentHashMap<>();
    private final AtomicLong futWrites = new AtomicLong();
    private final AtomicLong optWrites = new AtomicLong();

    private final Map<String, ConcurrentLinkedDeque<OptTick>> optionBuffers = new ConcurrentHashMap<>();

    public record OptTick(Instant ts, String instrumentKey, String symbol, double ltp, int qty, int oi) {}
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
                .subscribe(ev -> {
                    nseInstrumentService.refreshIfOptionsEmpty();
                    ensureOptionStream();
                    connectIfOpenOrSchedule();
                });

        auth.events()
                .filter(e -> e == UpstoxAuthService.AuthEvent.EXPIRED)
                .subscribe(e -> {
                    log.warn("Auth token expired or refresh failed; waiting for re-login");
                    orchestratorState.set(OrchestratorState.IDLE);
                    selectionComputed.set(false);
                    futLtpInitialized.set(false);
                });

        Flux.interval(Duration.ofSeconds(15))
                .subscribe(i -> {
                    boolean open = MarketHours.isOpen(Instant.now());
                    boolean wasOpen = marketWasOpen.getAndSet(open);
                    if (wasOpen && !open) {
                        onMarketClose();
                    }
                    boolean futOn = futSubscribed.get();
                    int optCount = optSubscribedCount.get();
                    long fut = futWrites.getAndSet(0);
                    long opt = optWrites.getAndSet(0);
                    ticksLast60s.set(fut + opt);
                    String source = connected.get() ? "live" : "influx";
                    if (open) {
                        log.info(
                                "HEARTBEAT market={} liveFut={} liveOpts={} writesFut={}/15s writesOpt={}/15s sourceUsed={} ceLoadedCount={} peLoadedCount={}",
                                "open", futOn ? "on" : "off", optCount, fut, opt, source,
                                ceLoadedCount.get(), peLoadedCount.get());
                    }
                });
    }

    public void connectIfOpenOrSchedule() {
        Instant now = Instant.now();
        ZoneId tz = MarketHours.zone();
        ZonedDateTime nowIst = now.atZone(tz);
        ZonedDateTime openIst = nowIst.with(MarketHours.openTime()).withSecond(0).withNano(0);
        ZonedDateTime closeIst = nowIst.with(MarketHours.closeTime()).withSecond(0).withNano(0);

        boolean isTradingWindowNow = MarketHours.isOpen(now);
        boolean todayIsTradingDay = MarketHours.isTradingDay(nowIst.toLocalDate());
        boolean tokenPresent = auth.currentToken() != null;
        boolean connected = orchestratorState.get() == OrchestratorState.READY;
        boolean shouldStart = isTradingWindowNow && todayIsTradingDay && tokenPresent && !connected;

        String decision = shouldStart ? "START" : "WAIT";

        if (shouldStart) {
            startLive();
        } else if (!isTradingWindowNow) {
            Instant next = MarketHours.nextOpenAfter(now);
            log.info("Market closed — scheduling live feed start at {}", next);
            long delay = Duration.between(now, next).toMillis();
            Mono.delay(Duration.ofMillis(delay)).subscribe(v -> startLive());
        }

        long nowMs = System.currentTimeMillis();
        long prev = lastAutoStartLog.get();
        if (nowMs - prev >= 60_000 && lastAutoStartLog.compareAndSet(prev, nowMs)) {
            log.info("AUTO-START-CHECK nowIst={} openIst={} closeIst={} isTradingWindowNow={} todayIsTradingDay={} tokenPresent={} connected={} decision={}",
                    nowIst.toLocalTime(), openIst.toLocalTime(), closeIst.toLocalTime(),
                    isTradingWindowNow, todayIsTradingDay, tokenPresent, connected, decision);
        }
    }

    public void startLive() {
        initLiveWebSocket();
    }

    private void ensureOptionStream() {
        NseInstrumentService.OptionBatch batch = nseInstrumentService.loadCurrentWeekOptionInstruments();
        ceLoadedCount.set(batch.ce().size());
        peLoadedCount.set(batch.pe().size());
        currentExpiryMs.set(batch.expiry());
        if (batch.ce().isEmpty() && batch.pe().isEmpty()) {
            log.info("OPTION-STREAM wait: no instruments yet — attempting JSON fallback");
            try {
                Double ltp = nseInstrumentService.getNearestExpiryNiftyFutureLtp().block(Duration.ofSeconds(5));
                if (ltp != null) {
                    NseInstrumentService.OptionBatch fb = nseInstrumentService.filterStrikesAroundLtpFromJson(ltp);
                    ceLoadedCount.set(fb.ce().size());
                    peLoadedCount.set(fb.pe().size());
                    currentExpiryMs.set(fb.expiry());
                }
            } catch (Exception ex) {
                log.error("⚠️ Failed JSON fallback for options", ex);
            }
            if (ceLoadedCount.get() == 0 && peLoadedCount.get() == 0) {
                Mono.delay(Duration.ofSeconds(30)).subscribe(i -> ensureOptionStream());
                return;
            }
        }
        streamFilteredNiftyOptions();
    }

    /**
     * Public entry to start or resume feeds ensuring option instruments exist.
     */
    public void startOrResume() {
        ZonedDateTime nowIst = ZonedDateTime.now(MarketHours.zone());
        nseInstrumentService.ensureOptionsLoaded(nowIst);
        startLive();
    }

    public void initLiveWebSocket() {
        if (mockMode) {
            log.info("🧪 Mock mode enabled - skipping live feed startup");
            return;
        }
        Instant now = Instant.now();
        if (!MarketHours.isOpen(now)) {
            log.info("Market closed — skipping WS connect until {}", MarketHours.nextOpenAfter(now));
            return;
        }
        if (!orchestratorState.compareAndSet(OrchestratorState.IDLE, OrchestratorState.RUNNING)) {
            return;
        }
        log.info("OAuth success — starting market pipeline...");
        try {
            if (instrumentsInitialized.compareAndSet(false, true)) {
                nseInstrumentService.ensureNseJsonLoaded();
                nseInstrumentService.purgeExpiredOptionDocs();
                nseInstrumentService.refreshIfOptionsEmpty();
                nseInstrumentService.saveNiftyFuturesToMongo();
            } else {
                log.info("init sequence skipped: already initialized");
            }
            streamNiftyFutAndTriggerCEPE();
        } catch (Exception e) {
            log.error("❌ init sequence failed", e);
        } finally {
            orchestratorState.set(OrchestratorState.READY);
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
        ReactorNettyWebSocketClient client = createWsClient();
        Sinks.Many<JsonNode> local = Sinks.many().multicast().onBackpressureBuffer();

        client.execute(URI.create(wsUrl), session -> {
            Flux<WebSocketMessage> pings = Flux.interval(Duration.ofSeconds(25))
                    .map(i -> session.pingMessage(factory -> factory.wrap(new byte[0])));
            session.send(pings).subscribe();

            return session.send(Mono.just(session.binaryMessage(bb -> bb.wrap(SUB_FRAME))))
                    .doOnSuccess(v -> log.info("▶︎ subscribe frame sent"))
                    .thenMany(session.receive()
                            .map(WebSocketMessage::getPayload)
                            .map(this::parseProtoFeedResponse)
                            .doOnNext(local::tryEmitNext))
                    .then();
        }).subscribe();

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
        ReactorNettyWebSocketClient client = createWsClient();
        Sinks.Many<JsonNode> local = Sinks.many().multicast().onBackpressureBuffer();

        client.execute(URI.create(wsUrl), session -> {
            Flux<WebSocketMessage> pings = Flux.interval(Duration.ofSeconds(25))
                    .map(i -> session.pingMessage(factory -> factory.wrap(new byte[0])));
            session.send(pings).subscribe();

            return session.send(Mono.just(session.binaryMessage(bb -> bb.wrap(subFrame))))
                    .doOnSuccess(v -> log.info("▶︎ Nifty options subscription frame sent"))
                    .thenMany(session.receive()
                            .map(WebSocketMessage::getPayload)
                            .map(this::parseProtoFeedResponse)
                            .doOnNext(local::tryEmitNext)
                            .doOnSubscribe(s -> log.info("📡 Subscribed to Nifty options WebSocket feed"))

                    )
                    .then();
        }).subscribe();

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
        ReactorNettyWebSocketClient client = createWsClient();
        Sinks.Many<JsonNode> local = Sinks.many().multicast().onBackpressureBuffer();

        client.execute(URI.create(wsUrl), session -> {
            Flux<WebSocketMessage> pings = Flux.interval(Duration.ofSeconds(30))
                    .map(i -> session.pingMessage(factory -> factory.wrap(new byte[0])));
            session.send(pings).subscribe();

            return session.send(Mono.just(session.binaryMessage(bb -> bb.wrap(OPTION_SUB_FRAME))))
                    .doOnSuccess(v -> log.info("▶︎ option‐subscribe frame sent"))
                    .thenMany(session.receive()
                            .map(WebSocketMessage::getPayload)
                            .map(this::parseProtoFeedResponse)
                            .doOnNext(local::tryEmitNext))
                    .then();
        }).subscribe();

        return local.asFlux();
    }

    private ReactorNettyWebSocketClient createWsClient() {
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.SO_KEEPALIVE, true)
                .responseTimeout(Duration.ofSeconds(20));
        return new ReactorNettyWebSocketClient(httpClient);
    }

    private int nextDelay(int attempt) {
        return switch (attempt) {
            case 1 -> 2;
            case 2 -> 5;
            case 3 -> 10;
            case 4 -> 20;
            default -> 30;
        };
    }

public void streamFilteredNiftyOptions() {
    NseInstrumentService.SelectionData sel = nseInstrumentService.currentSelectionData();
    ceLoadedCount.set(sel.ceCount());
    peLoadedCount.set(sel.peCount());
    currentExpiryMs.set(sel.expiry());
    List<String> desired = sel.keys();
    String futKey = nseInstrumentService.nearestNiftyFutureKey().orElse(null);
    if (futKey != null) {
        futInstrumentKey.set(futKey);
    }
    List<String> optionKeys = desired.stream()
            .filter(k -> !k.equals(futKey))
            .toList();
    if (desired.isEmpty()) {
        log.info("OPTION-STREAM wait: no instruments yet (will retry)");
        optionsStreamStarted.set(false);
        Mono.delay(Duration.ofSeconds(30)).subscribe(i -> streamFilteredNiftyOptions());
        return;
    }
    Set<String> toAdd = new HashSet<>(desired);
    toAdd.removeAll(currentlySubscribedKeys);
    Set<String> toRemove = new HashSet<>(currentlySubscribedKeys);
    toRemove.removeAll(new HashSet<>(desired));

    if (toAdd.isEmpty() && toRemove.isEmpty() && optionsStreamStarted.get()) {
        log.info("Orchestration skipped — selection unchanged (expiry={})",
                nseInstrumentService.formatExpiry(sel.expiry()));
        return;
    }

    currentlySubscribedKeys.addAll(toAdd);
    currentlySubscribedKeys.removeAll(toRemove);
    log.info("Subscriptions updated: +{} / -{} (total={})", toAdd.size(), toRemove.size(), desired.size());

    if (!optionsStreamStarted.compareAndSet(false, true)) {
        return;
    }

    log.info("OPTION-STREAM started keys={} expiry={}", desired.size(), sel.expiry());

    auth.ensureValidToken()
        .flatMap(valid -> {
            if (!valid) {
                optionsStreamStarted.set(false);
                log.warn("⚠️ Upstox token not ready — skipping option stream start");
                return Mono.empty();
            }
            return fetchWebSocketUrl();
        })
        .flatMapMany(wsUrl -> openWebSocketWithDynamicSub(wsUrl, () -> buildSubFrame(optionKeys), optionKeys.size()))
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
                    logLtp(instrumentKey, ltp, ts);
                    writeTickToInflux(instrumentKey, feed, ts);
                    lastTick.put(instrumentKey, new Tick(instrumentKey, ltp, Instant.ofEpochMilli(ts)));
                    bufferOptionTick(instrumentKey, feed, ts, ltp);

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
            optionsStreamStarted.set(false);
            log.error("❌ filtered option feed failed:", err);
        })
        .doFinally(sig -> {
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
                    logLtp(instrumentKey, ltp, ts);
                    ltpSink.tryEmitNext(new LtpEvent(instrumentKey, ltp, Instant.ofEpochMilli(ts)));
                    writeTickToInflux(instrumentKey, tick.path("feeds").path(instrumentKey), ts);
                    lastTick.put(instrumentKey, new Tick(instrumentKey, ltp, Instant.ofEpochMilli(ts)));
                    bufferOptionTick(instrumentKey, tick.path("feeds").path(instrumentKey), ts, ltp);
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

private byte[] buildSubFrame(List<String> keys) {
    ObjectNode frame = om.createObjectNode();
    frame.put("guid", "filtered-options-guid");
    frame.put("method", "sub");

    ObjectNode data = frame.putObject("data");
    data.put("mode", "full");
    ArrayNode arr = data.putArray("instrumentKeys");
    keys.forEach(arr::add);

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
    futLtpInitialized.set(false);
    long start = System.currentTimeMillis();
    AtomicBoolean warned = new AtomicBoolean(false);

    fetchWebSocketUrl()
            .flatMapMany(wsUrl -> openWebSocketForOptions(wsUrl, buildSubFrame(instrumentKey)))
            .doOnSubscribe(s -> futSubscribed.set(true))
            .doFinally(sig -> futSubscribed.set(false))
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
                        logLtp(instrumentKey, ltp, ts);
                        writeTickToInflux(instrumentKey, feed, ts);
                        lastTick.put(instrumentKey, new Tick(instrumentKey, ltp, Instant.ofEpochMilli(ts)));
                        bufferOptionTick(instrumentKey, feed, ts, ltp);

                        if (futLtpInitialized.compareAndSet(false, true)) {
                            selectionComputed.set(true);
                            nseInstrumentService.filterStrikesAroundLtpFromJson(ltp);
                            NseInstrumentService.SelectionData sel = nseInstrumentService.currentSelectionData();
                            String sig = nseInstrumentService.selectionSignature(sel);
                            if (sig.equals(lastSelectionSignature.get())) {
                                log.info("Orchestration skipped — selection unchanged (expiry={})", nseInstrumentService.formatExpiry(sel.expiry()));
                            } else {
                                lastSelectionSignature.set(sig);
                                streamFilteredNiftyOptions();
                                log.info("Setup complete — FUT={} expiry={}; CE={}, PE={}; subscribed={}",
                                        instrumentKey,
                                        nseInstrumentService.formatExpiry(sel.expiry()),
                                        sel.ceCount(), sel.peCount(), sel.keys().size());
                            }
                        }
                    } else if (!futLtpInitialized.get()) {
                        long elapsed = System.currentTimeMillis() - start;
                        if (elapsed > 500 && warned.compareAndSet(false, true)) {
                            log.warn("⚠️ LTP not found in tick — instrumentKey={}", instrumentKey);
                        } else {
                            log.debug("Waiting for FUT LTP...");
                        }
                    }
                } catch (Exception ex) {
                    log.error("⚠️ Failed to extract LTP or trigger filtering", ex);
                }
            })
            .doOnError(err -> log.error("❌ WebSocket stream failed:", err))
            .subscribe();
}
/** Builds a fresh SUB frame from the current filtered_nifty_premiums (15 CE + 15 PE). */
/**
 * Opens a WS and sends a fresh SUB frame per connection (frameSupplier is called on every connect).
 * Use this for dynamic lists that may change between reconnects.
 */
private Flux<JsonNode> openWebSocketWithDynamicSub(String wsUrl, java.util.function.Supplier<byte[]> frameSupplier, int subsCount) {
    ReactorNettyWebSocketClient client = createWsClient();
    Sinks.Many<JsonNode> local = Sinks.many().multicast().onBackpressureBuffer();
    AtomicInteger attempt = new AtomicInteger(0);

    Runnable connect = new Runnable() {
        @Override
        public void run() {
            if (wsConnecting.getAndSet(true) || connected.get()) {
                return; // guard against parallel connects
            }
            client.execute(URI.create(wsUrl), session -> {
                Flux<WebSocketMessage> pings = Flux.interval(Duration.ofSeconds(25))
                        .map(i -> session.pingMessage(factory -> factory.wrap(new byte[0])));
                session.send(pings).subscribe();

                Flux<WebSocketMessage> subs;
                String futKey = futInstrumentKey.get();
                if (futKey != null) {
                    subs = Flux.concat(
                            Mono.just(session.binaryMessage(bb -> bb.wrap(buildSubFrame(futKey)))),
                            Mono.just(session.binaryMessage(bb -> bb.wrap(frameSupplier.get()))));
                } else {
                    subs = Flux.just(session.binaryMessage(bb -> bb.wrap(frameSupplier.get())));
                }

                return session.send(subs)
                        .doOnSuccess(v -> {
                            futSubscribed.set(futKey != null);
                            int total = subsCount + (futSubscribed.get() ? 1 : 0);
                            if (connected.compareAndSet(false, true)) {
                                if (everConnected.getAndSet(true)) {
                                    log.info("LIVE RECONNECTED");
                                }
                                log.info("LIVE CONNECTED (subs={})", total);
                            }
                            optSubscribedCount.set(subsCount);
                            attempt.set(0);
                            wsConnecting.set(false);
                        })
                        .thenMany(session.receive()
                                .map(WebSocketMessage::getPayload)
                                .map(LiveFeedService.this::parseProtoFeedResponse)
                                .doOnNext(local::tryEmitNext))
                        .doOnError(err -> log.error("❌ WebSocket stream failed:", err))
                        .doFinally(sig -> {
                            connected.set(false);
                            wsConnecting.set(false);
                            futSubscribed.set(false);
                            optSubscribedCount.set(0);
                            int delay = nextDelay(attempt.incrementAndGet());
                            log.info("LIVE DISCONNECTED → reconnecting in {}s", delay);
                            Mono.delay(Duration.ofSeconds(delay)).subscribe(i -> run());
                        })
                        .then();
            }).subscribe();
        }
    };

    connect.run();

    return local.asFlux();
}

    private void logLtp(String instrumentKey, double ltp, long ts) {
        logResolvedLtp(instrumentKey, ltp, "live");
    }

    public void logResolvedLtp(String instrumentKey, double ltp, String source) {
        NseInstrument info = instrumentCache.computeIfAbsent(instrumentKey,
                k -> mongoTemplate.findById(k, NseInstrument.class));
        String label = instrumentKey;
        if (info != null) {
            String type = info.getInstrumentType();
            if (type != null && type.toUpperCase().contains("FUT")) {
                label = "NIFTY FUT";
            } else if ("CE".equalsIgnoreCase(type) || "PE".equalsIgnoreCase(type)) {
                Integer sp = info.getStrikePrice();
                String strike = sp != null ? String.valueOf(sp) : "";
                label = String.format("NIFTY %s strike=%s", type.toUpperCase(), strike);
            } else if (info.getTradingSymbol() != null) {
                label = info.getTradingSymbol();
            }
        }
        if ("NIFTY FUT".equals(label)) {
            log.info("ltp for nifty future = {} ({}).", ltp, source);
        } else {
            log.info("LTP [{}] {}={}", label, source, ltp);
        }
    }

    private void onMarketClose() {
        double futPrice = 0.0;
        long optRows = 0;
        String futKey = nseInstrumentService.nearestNiftyFutureKey().orElse(null);
        if (futKey != null) {
            Tick t = lastTick.get(futKey);
            if (t != null) {
                ObjectNode feed = om.createObjectNode().put("ltp", t.ltp());
                writeTickToInflux(futKey, feed, t.ts().toEpochMilli());
                logLtp(futKey, t.ltp(), t.ts().toEpochMilli());
                futPrice = t.ltp();
            }
        }
        for (var dq : optionBuffers.values()) {
            OptTick t = dq.peekFirst();
            if (t == null) continue;
            ObjectNode feed = om.createObjectNode().put("ltp", t.ltp());
            if (t.qty() > 0) feed.put("qty", t.qty());
            if (t.oi() > 0) feed.put("oi", t.oi());
            writeTickToInflux(t.instrumentKey(), feed, t.ts().toEpochMilli());
            logLtp(t.instrumentKey(), t.ltp(), t.ts().toEpochMilli());
            optRows++;
        }
        log.info("MARKET CLOSED — last snapshot FUT={}, OPT rows={}", futPrice, optRows);
        optionBuffers.clear();
        connected.set(false);
        futSubscribed.set(false);
        optSubscribedCount.set(0);
    }

    private void writeTickToInflux(String instrumentKey, JsonNode feed, long ts) {
        lastTickTs.set(Instant.ofEpochMilli(ts));
        if (writeApi == null) {
            log.debug("Skipping Influx write (no client)");
            return;
        }
        NseInstrument info = instrumentCache.computeIfAbsent(instrumentKey,
                k -> mongoTemplate.findById(k, NseInstrument.class));
        if (info == null) {
            log.debug("Skipping Influx write (unknown instrument): {}", instrumentKey);
            return;
        }
        boolean isFut = info.getInstrumentType() != null && info.getInstrumentType().toUpperCase().contains("FUT");
        String measurement = isFut ? "nifty_fut_ltp" : "nifty_option_ticks";

        long exp = info.getExpiry();
        if (exp < 1_000_000_000_000L) exp *= 1000L;
        String expiry = Instant.ofEpochMilli(exp).toString().substring(0, 10);

        Point p = Point.measurement(measurement)
                .addTag("instrumentKey", instrumentKey)
                .addTag("symbol", "NIFTY")
                .addTag("segment", isFut ? "FUTIDX" : "OPTIDX")
                .addTag("expiry", expiry);

        String t = info.getInstrumentType();
        if (!isFut && t != null) {
            p.addTag("type", t.toUpperCase());
            Integer sp = info.getStrikePrice();
            int strike = (sp != null) ? sp : 0;
            p.addTag("strike", String.valueOf(strike));
        }

        JsonNode ltpNode = findField(feed, "ltp");
        if (ltpNode != null && ltpNode.isNumber()) {
            p.addField("ltp", ltpNode.asDouble());
        }
        int qty = findIntField(feed, "qty");
        int oi = findIntField(feed, "oi");
        if (qty != 0) p.addField("qty", qty);
        if (oi != 0) p.addField("oi", oi);

        p.time(Instant.ofEpochMilli(ts), WritePrecision.MS);

        try {
            writeApi.writePoint(influxBucket, influxOrg, p);
            if (isFut) futWrites.incrementAndGet(); else optWrites.incrementAndGet();
        } catch (Exception e) {
            log.debug("Influx write failed for {}: {}", instrumentKey, e.getMessage());
        }
    }

    private void bufferOptionTick(String instrumentKey, JsonNode feed, long ts, double ltp) {
        NseInstrument info = instrumentCache.computeIfAbsent(instrumentKey,
                k -> mongoTemplate.findById(k, NseInstrument.class));
        if (info == null) return;
        String type = info.getInstrumentType();
        if (type == null || !(type.equalsIgnoreCase("CE") || type.equalsIgnoreCase("PE"))) return;
        String symbol = info.getTradingSymbol();
        if (symbol == null) return;
        int qty = findIntField(feed, "qty");
        int oi = findIntField(feed, "oi");
        OptTick tick = new OptTick(Instant.ofEpochMilli(ts), instrumentKey, symbol, ltp, qty, oi);
        ConcurrentLinkedDeque<OptTick> dq = optionBuffers.computeIfAbsent(symbol, k -> new ConcurrentLinkedDeque<>());
        dq.addFirst(tick);
        while (dq.size() > 200) {
            dq.removeLast();
        }
    }

    private int findIntField(JsonNode node, String name) {
        JsonNode n = findField(node, name);
        return n != null && n.isNumber() ? n.intValue() : 0;
    }

    private JsonNode findField(JsonNode node, String name) {
        if (node.has(name) && node.get(name).isNumber()) {
            return node.get(name);
        }
        var it = node.fields();
        while (it.hasNext()) {
            var e = it.next();
            JsonNode val = e.getValue();
            if (val.isObject()) {
                JsonNode found = findField(val, name);
                if (found != null) return found;
            }
        }
        return null;
    }

    public List<OptTick> recentOptionTicks(String symbol) {
        ConcurrentLinkedDeque<OptTick> dq = optionBuffers.get(symbol);
        if (dq == null) return List.of();
        return new ArrayList<>(dq);
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


}
