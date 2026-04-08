package com.wikipedia.monitor.controller;

import com.wikipedia.monitor.model.GoogleTrend;
import com.wikipedia.monitor.service.GoogleTrendsService;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/api/google")
public class GoogleTrendsController {

    private final GoogleTrendsService googleTrendsService;

    public GoogleTrendsController(GoogleTrendsService googleTrendsService) {
        this.googleTrendsService = googleTrendsService;
    }

    @GetMapping("/trends")
    public Mono<List<GoogleTrend>> getTrends(@RequestParam(defaultValue = "US") String geo) {
        return googleTrendsService.fetchTrends(geo);
    }

    @GetMapping(value = "/trends/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<List<GoogleTrend>>> streamTrends(
            @RequestParam(defaultValue = "US") String geo) {

        return googleTrendsService.trendStream(geo)
                .map(trends -> ServerSentEvent.<List<GoogleTrend>>builder()
                        .event("trends")
                        .data(trends)
                        .build());
    }
}
