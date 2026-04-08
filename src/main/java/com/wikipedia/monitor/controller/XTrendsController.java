package com.wikipedia.monitor.controller;

import com.wikipedia.monitor.model.XTrend;
import com.wikipedia.monitor.service.XTrendsService;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/x")
public class XTrendsController {

    private final XTrendsService xTrendsService;

    public XTrendsController(XTrendsService xTrendsService) {
        this.xTrendsService = xTrendsService;
    }

    /** One-shot fetch — used on page load and manual refresh. */
    @GetMapping("/trends")
    public Mono<Map<String, Object>> getTrends(@RequestParam(defaultValue = "1") int woeid) {
        if (!xTrendsService.isConfigured()) {
            return Mono.just(Map.of("configured", false, "trends", List.of()));
        }
        return xTrendsService.fetchTrends(woeid)
                .map(trends -> Map.of("configured", true, "trends", trends, "woeid", woeid));
    }

    /** SSE stream — pushes a new snapshot every 5 minutes. */
    @GetMapping(value = "/trends/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Map<String, Object>>> streamTrends(
            @RequestParam(defaultValue = "1") int woeid) {

        if (!xTrendsService.isConfigured()) {
            return Flux.just(ServerSentEvent.<Map<String, Object>>builder()
                    .event("config-error")
                    .data(Map.of("configured", false))
                    .build());
        }
        return xTrendsService.trendStream(woeid)
                .map(trends -> ServerSentEvent.<Map<String, Object>>builder()
                        .event("trends")
                        .data(Map.of("configured", true, "trends", trends, "woeid", woeid))
                        .build());
    }
}
