package com.wikipedia.monitor.controller;

import com.wikipedia.monitor.model.TwitterTrend;
import com.wikipedia.monitor.service.TwitterTrendsService;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.List;

@RestController
@RequestMapping("/api/twitter")
public class TwitterTrendsController {

    private final TwitterTrendsService service;

    public TwitterTrendsController(TwitterTrendsService service) {
        this.service = service;
    }

    @GetMapping("/trends")
    public Mono<List<TwitterTrend>> getTrends(
            @RequestParam(defaultValue = "worldwide") String country) {
        return service.getTrends(country);
    }
}
