package com.wikipedia.monitor.controller;

import com.wikipedia.monitor.model.BlueskyPost;
import com.wikipedia.monitor.service.BlueskyFirehoseService;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;

@RestController
public class BlueskyController {

    private final BlueskyFirehoseService firehoseService;

    public BlueskyController(BlueskyFirehoseService firehoseService) {
        this.firehoseService = firehoseService;
    }

    @GetMapping(value = "/stream/bluesky", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<BlueskyPost>> streamPosts(
            @RequestParam(required = false) String lang) {

        Flux<BlueskyPost> source = (lang != null && !lang.isBlank())
                ? firehoseService.getPostStreamForLang(lang)
                : firehoseService.getPostStream();

        Flux<ServerSentEvent<BlueskyPost>> posts = source
                .filter(p -> p.text() != null && !p.text().isBlank())
                .map(post -> ServerSentEvent.<BlueskyPost>builder()
                        .event("post")
                        .data(post)
                        .build());

        Flux<ServerSentEvent<BlueskyPost>> heartbeat = Flux.interval(Duration.ofSeconds(30))
                .map(tick -> ServerSentEvent.<BlueskyPost>builder().comment("keepalive").build());

        return Flux.merge(posts, heartbeat);
    }
}
