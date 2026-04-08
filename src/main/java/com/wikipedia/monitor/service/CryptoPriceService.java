package com.wikipedia.monitor.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.monitor.model.CryptoTick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.time.Duration;
import java.util.List;

@Service
public class CryptoPriceService {

    private static final Logger log = LoggerFactory.getLogger(CryptoPriceService.class);

    private static final List<String> SYMBOLS = List.of(
            "btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt",
            "dogeusdt", "adausdt", "avaxusdt", "linkusdt", "dotusdt",
            "ltcusdt", "uniusdt", "atomusdt", "nearusdt", "maticusdt"
    );

    private static final String WS_URL = "wss://stream.binance.com:9443/stream?streams="
            + String.join("/", SYMBOLS.stream().map(s -> s + "@miniTicker").toList());

    private final Sinks.Many<CryptoTick> sink;
    private final Flux<CryptoTick> sharedFlux;
    private final ObjectMapper objectMapper;
    private final ReactorNettyWebSocketClient wsClient;

    public CryptoPriceService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.wsClient = new ReactorNettyWebSocketClient();
        this.sink = Sinks.many().multicast().onBackpressureBuffer(500, false);
        this.sharedFlux = sink.asFlux().share();
        connect();
    }

    private void connect() {
        attemptConnection()
                .retryWhen(reactor.util.retry.Retry
                        .backoff(Long.MAX_VALUE, Duration.ofSeconds(3))
                        .maxBackoff(Duration.ofSeconds(30))
                        .doBeforeRetry(s -> log.warn("Reconnecting to Binance stream, attempt {}", s.totalRetries() + 1)))
                .subscribe(null, err -> log.error("Fatal Binance connection error", err));
    }

    private Mono<Void> attemptConnection() {
        return wsClient.execute(URI.create(WS_URL), session ->
                session.receive()
                        .map(msg -> msg.getPayloadAsText())
                        .flatMap(this::parseTick)
                        .doOnNext(tick -> sink.tryEmitNext(tick))
                        .then()
        );
    }

    private Flux<CryptoTick> parseTick(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            JsonNode data = root.get("data");
            if (data == null) return Flux.empty();

            String symbol = data.get("s").asText();
            double price  = Double.parseDouble(data.get("c").asText());
            double open   = Double.parseDouble(data.get("o").asText());
            double high   = Double.parseDouble(data.get("h").asText());
            double low    = Double.parseDouble(data.get("l").asText());
            double volume = Double.parseDouble(data.get("v").asText());

            return Flux.just(new CryptoTick(symbol, price, open, high, low, volume));
        } catch (Exception e) {
            return Flux.empty();
        }
    }

    public Flux<CryptoTick> getTickStream() {
        return sharedFlux;
    }
}
