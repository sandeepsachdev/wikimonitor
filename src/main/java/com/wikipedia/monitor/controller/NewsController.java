package com.wikipedia.monitor.controller;

import com.wikipedia.monitor.model.NewsItem;
import com.wikipedia.monitor.service.NewsAggregatorService;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;

@RestController
public class NewsController {

    private final NewsAggregatorService newsAggregatorService;

    public NewsController(NewsAggregatorService newsAggregatorService) {
        this.newsAggregatorService = newsAggregatorService;
    }

    @GetMapping(value = "/stream/news", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<NewsItem>> streamNews() {
        Flux<ServerSentEvent<NewsItem>> news = newsAggregatorService.getNewsStream()
                .map(item -> ServerSentEvent.<NewsItem>builder()
                        .event("news")
                        .data(item)
                        .build());

        // Heartbeat every 30 s. Attempting to write to a dead connection fails
        // and causes the subscription to be cancelled, freeing its resources.
        Flux<ServerSentEvent<NewsItem>> heartbeat = Flux.interval(Duration.ofSeconds(30))
                .map(tick -> ServerSentEvent.<NewsItem>builder().comment("keepalive").build());

        return Flux.merge(news, heartbeat);
    }
}
