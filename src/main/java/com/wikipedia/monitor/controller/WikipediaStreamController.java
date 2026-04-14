package com.wikipedia.monitor.controller;

import com.wikipedia.monitor.model.WikipediaEdit;
import com.wikipedia.monitor.service.WikipediaStreamService;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

@RestController
public class WikipediaStreamController {

    private final WikipediaStreamService streamService;
    private final AtomicLong counter = new AtomicLong();

    public WikipediaStreamController(WikipediaStreamService streamService) {
        this.streamService = streamService;
    }

    @GetMapping(value = "/stream/edits", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<WikipediaEdit>> streamEdits(
            @RequestParam(required = false) String wiki) {

        Flux<WikipediaEdit> source = (wiki != null && !wiki.isBlank())
                ? streamService.getEditStreamForWiki(wiki)
                : streamService.getEditStream();

        Flux<ServerSentEvent<WikipediaEdit>> edits = source
                .map(edit -> ServerSentEvent.<WikipediaEdit>builder()
                        .id(String.valueOf(counter.incrementAndGet()))
                        .event("edit")
                        .data(edit)
                        .build());

        Flux<ServerSentEvent<WikipediaEdit>> heartbeat = Flux.interval(Duration.ofSeconds(30))
                .map(tick -> ServerSentEvent.<WikipediaEdit>builder().comment("keepalive").build());

        return Flux.merge(edits, heartbeat);
    }
}
