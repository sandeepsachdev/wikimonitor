package com.wikipedia.monitor.controller;

import com.wikipedia.monitor.model.TrendsSnapshot;
import com.wikipedia.monitor.service.TrendsService;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;

@RestController
public class TrendsController {

    private final TrendsService trendsService;

    public TrendsController(TrendsService trendsService) {
        this.trendsService = trendsService;
    }

    @GetMapping(value = "/stream/trends", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<TrendsSnapshot>> streamTrends() {
        return Flux.interval(Duration.ofSeconds(3))
                .map(tick -> ServerSentEvent.<TrendsSnapshot>builder()
                        .event("trends")
                        .data(trendsService.snapshot())
                        .build());
    }
}
