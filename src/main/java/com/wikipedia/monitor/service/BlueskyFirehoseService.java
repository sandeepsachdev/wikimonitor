package com.wikipedia.monitor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.monitor.model.BlueskyPost;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

import java.net.URI;
import java.time.Duration;

@Service
public class BlueskyFirehoseService {

    private static final Logger log = LoggerFactory.getLogger(BlueskyFirehoseService.class);

    // Jetstream: lightweight JSON WebSocket firehose (no auth, no CBOR decoding needed)
    private static final String JETSTREAM_URL =
            "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post";

    private static final int SINK_BUFFER = 512;

    private final Sinks.Many<BlueskyPost> sink;
    private final Flux<BlueskyPost> sharedFlux;
    private final ObjectMapper objectMapper;
    private final ReactorNettyWebSocketClient wsClient;

    public BlueskyFirehoseService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.wsClient = new ReactorNettyWebSocketClient();
        this.sink = Sinks.many().multicast().onBackpressureBuffer(SINK_BUFFER, false);
        this.sharedFlux = sink.asFlux().share();

        connect();
    }

    private void connect() {
        attemptConnection()
                .retryWhen(reactor.util.retry.Retry
                        .backoff(Long.MAX_VALUE, Duration.ofSeconds(2))
                        .maxBackoff(Duration.ofSeconds(30))
                        .doBeforeRetry(s -> log.warn("Reconnecting to Bluesky Jetstream, attempt {}", s.totalRetries() + 1)))
                .subscribe(
                        null,
                        err -> log.error("Fatal Bluesky connection error", err)
                );
    }

    private reactor.core.publisher.Mono<Void> attemptConnection() {
        return wsClient.execute(URI.create(JETSTREAM_URL), session ->
                session.receive()
                        // Sample BEFORE text extraction so un-sampled frames are released
                        // immediately without allocating String objects. The firehose sends
                        // thousands of messages per second; this caps processing to ~20/sec.
                        .sample(Duration.ofMillis(50))
                        .map(msg -> msg.getPayloadAsText())
                        .flatMap(this::parsePost)
                        .filter(BlueskyPost::isCreate)
                        .doOnNext(post -> sink.tryEmitNext(post))
                        .then()
        );
    }

    private Flux<BlueskyPost> parsePost(String json) {
        try {
            return Flux.just(objectMapper.readValue(json, BlueskyPost.class));
        } catch (Exception e) {
            return Flux.empty();
        }
    }

    public Flux<BlueskyPost> getPostStream() {
        return sharedFlux;
    }

    public Flux<BlueskyPost> getPostStreamForLang(String lang) {
        return sharedFlux.filter(p -> {
            var langs = p.langs();
            return langs != null && langs.contains(lang);
        });
    }
}
