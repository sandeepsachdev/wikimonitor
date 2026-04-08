package com.wikipedia.monitor.service;

import com.wikipedia.monitor.model.XTrend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

@Service
public class XTrendsService {

    private static final Logger log = LoggerFactory.getLogger(XTrendsService.class);
    private static final String X_API_BASE = "https://api.twitter.com";
    private static final Duration POLL_INTERVAL = Duration.ofMinutes(5);

    private final String bearerToken;
    private final WebClient webClient;

    // Cached result
    private volatile CachedTrends cache = new CachedTrends(Collections.emptyList(), null, null);

    public XTrendsService(@Value("${x.api.bearer-token:}") String bearerToken) {
        this.bearerToken = bearerToken;
        this.webClient = WebClient.builder()
                .baseUrl(X_API_BASE)
                .defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + bearerToken)
                .codecs(c -> c.defaultCodecs().maxInMemorySize(1024 * 1024))
                .build();
    }

    public boolean isConfigured() {
        return bearerToken != null && !bearerToken.isBlank();
    }

    public Mono<List<XTrend>> fetchTrends(int woeid) {
        if (!isConfigured()) {
            return Mono.just(Collections.emptyList());
        }
        return webClient.get()
                .uri("/2/trends/by/woeid/{woeid}", woeid)
                .retrieve()
                .bodyToMono(XTrend.ApiResponse.class)
                .map(response -> response.data() != null ? response.data() : Collections.<XTrend>emptyList())
                .doOnNext(trends -> cache = new CachedTrends(trends, woeid, Instant.now()))
                .onErrorResume(WebClientResponseException.class, ex -> {
                    if (ex.getStatusCode() == HttpStatus.TOO_MANY_REQUESTS) {
                        log.warn("X API rate limit hit, returning cached trends");
                    } else {
                        log.error("X API error {}: {}", ex.getStatusCode(), ex.getResponseBodyAsString());
                    }
                    return Mono.just(cache.trends());
                })
                .onErrorResume(ex -> {
                    log.error("Failed to fetch X trends: {}", ex.getMessage());
                    return Mono.just(cache.trends());
                });
    }

    /**
     * SSE stream: emits fresh trends every POLL_INTERVAL, then on each interval tick.
     */
    public Flux<List<XTrend>> trendStream(int woeid) {
        return Flux.interval(Duration.ZERO, POLL_INTERVAL)
                .flatMap(tick -> fetchTrends(woeid));
    }

    public CachedTrends getCache() {
        return cache;
    }

    public record CachedTrends(List<XTrend> trends, Integer woeid, Instant fetchedAt) {}
}
