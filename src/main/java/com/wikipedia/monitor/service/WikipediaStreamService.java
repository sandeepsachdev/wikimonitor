package com.wikipedia.monitor.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wikipedia.monitor.model.WikipediaEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

import java.time.Duration;

@Service
public class WikipediaStreamService {

    private static final Logger log = LoggerFactory.getLogger(WikipediaStreamService.class);
    private static final String WIKIPEDIA_STREAM_URL =
            "https://stream.wikimedia.org/v2/stream/recentchange";

    private final WebClient webClient = WebClient.builder()
            .codecs(config -> config.defaultCodecs().maxInMemorySize(4 * 1024 * 1024))
            .build();

    private final Sinks.Many<WikipediaEdit> sink;
    private final Flux<WikipediaEdit> sharedFlux;
    private final ObjectMapper objectMapper;

    public WikipediaStreamService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.sink = Sinks.many().multicast().onBackpressureBuffer(512, false);
        this.sharedFlux = sink.asFlux().share();

        connectToWikipediaStream();
    }

    private void connectToWikipediaStream() {
        webClient.get()
                .uri(WIKIPEDIA_STREAM_URL)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .retrieve()
                .bodyToFlux(new ParameterizedTypeReference<ServerSentEvent<String>>() {})
                .flatMap(event -> {
                    String data = event.data();
                    if (data == null || data.isBlank()) return Flux.empty();
                    return parseEdit(data);
                })
                .filter(edit -> "edit".equals(edit.type()) || "new".equals(edit.type()))
                // Wikipedia can burst to 50+ edits/sec; cap at ~20/sec to avoid
                // flooding downstream subscribers and the sink buffer.
                .sample(Duration.ofMillis(50))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(3))
                        .maxBackoff(Duration.ofSeconds(30))
                        .doBeforeRetry(signal ->
                                log.warn("Reconnecting to Wikipedia stream, attempt {}", signal.totalRetries() + 1)))
                .subscribe(
                        edit -> sink.tryEmitNext(edit),
                        error -> log.error("Fatal error in Wikipedia stream", error)
                );
    }

    private Flux<WikipediaEdit> parseEdit(String json) {
        try {
            WikipediaEdit edit = objectMapper.readValue(json, WikipediaEdit.class);
            return Flux.just(edit);
        } catch (Exception e) {
            return Flux.empty();
        }
    }

    public Flux<WikipediaEdit> getEditStream() {
        return sharedFlux;
    }

    public Flux<WikipediaEdit> getEditStreamForWiki(String wiki) {
        return sharedFlux.filter(edit -> wiki.equals(edit.wiki()));
    }
}
