package com.wikipedia.monitor.controller;

import com.wikipedia.monitor.model.NewsItem;
import com.wikipedia.monitor.service.NewsAggregatorService;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
public class NewsController {

    private final NewsAggregatorService newsAggregatorService;

    public NewsController(NewsAggregatorService newsAggregatorService) {
        this.newsAggregatorService = newsAggregatorService;
    }

    @GetMapping(value = "/stream/news", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<NewsItem>> streamNews() {
        return newsAggregatorService.getNewsStream()
                .map(item -> ServerSentEvent.<NewsItem>builder()
                        .event("news")
                        .data(item)
                        .build());
    }
}
