package com.wikipedia.monitor.service;

import com.wikipedia.monitor.model.GoogleTrend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class GoogleTrendsService {

    private static final Logger log = LoggerFactory.getLogger(GoogleTrendsService.class);
    private static final String RSS_BASE = "https://trends.google.com/trending/rss";
    private static final Duration POLL_INTERVAL = Duration.ofMinutes(15);

    // Patterns for RSS XML parsing (no XML library needed — the feed is simple/consistent)
    private static final Pattern ITEM_P    = Pattern.compile("<item>(.*?)</item>", Pattern.DOTALL);
    private static final Pattern TITLE_P   = Pattern.compile("<title><!\\[CDATA\\[(.*?)]]></title>|<title>(.*?)</title>");
    private static final Pattern TRAFFIC_P = Pattern.compile("<ht:approx_traffic>(.*?)</ht:approx_traffic>");
    private static final Pattern PUBDATE_P = Pattern.compile("<pubDate>(.*?)</pubDate>");
    private static final Pattern NEWS_P    = Pattern.compile("<ht:news_item>(.*?)</ht:news_item>", Pattern.DOTALL);
    private static final Pattern NEWS_TITLE_P   = Pattern.compile("<ht:news_item_title><!\\[CDATA\\[(.*?)]]></ht:news_item_title>|<ht:news_item_title>(.*?)</ht:news_item_title>");
    private static final Pattern NEWS_URL_P     = Pattern.compile("<ht:news_item_url><!\\[CDATA\\[(.*?)]]></ht:news_item_url>|<ht:news_item_url>(.*?)</ht:news_item_url>");
    private static final Pattern NEWS_SOURCE_P  = Pattern.compile("<ht:news_item_source><!\\[CDATA\\[(.*?)]]></ht:news_item_source>|<ht:news_item_source>(.*?)</ht:news_item_source>");
    private static final Pattern NEWS_SNIPPET_P = Pattern.compile("<ht:news_item_snippet><!\\[CDATA\\[(.*?)]]></ht:news_item_snippet>|<ht:news_item_snippet>(.*?)</ht:news_item_snippet>");

    private record CacheEntry(List<GoogleTrend> trends, Instant fetchedAt) {}

    private final WebClient webClient;
    // Per-geo result cache — avoids redundant HTTP calls when multiple clients
    // request the same geo within the same poll interval.
    private final Map<String, CacheEntry> geoCache = new ConcurrentHashMap<>();
    // Per-geo shared stream — all SSE subscribers for the same geo share one
    // Flux.interval rather than each creating their own polling loop.
    private final Map<String, Flux<List<GoogleTrend>>> sharedStreams = new ConcurrentHashMap<>();

    public GoogleTrendsService() {
        this.webClient = WebClient.builder()
                .defaultHeader("User-Agent", "Mozilla/5.0 (compatible; WikipediaMonitor/1.0)")
                .codecs(c -> c.defaultCodecs().maxInMemorySize(4 * 1024 * 1024))
                .build();
    }

    public Mono<List<GoogleTrend>> fetchTrends(String geo) {
        CacheEntry entry = geoCache.get(geo);
        if (entry != null &&
                Duration.between(entry.fetchedAt(), Instant.now()).compareTo(POLL_INTERVAL) < 0) {
            return Mono.just(entry.trends());
        }
        return webClient.get()
                .uri(RSS_BASE + "?geo=" + geo)
                .retrieve()
                .bodyToMono(String.class)
                .map(this::parseRss)
                .doOnNext(trends -> geoCache.put(geo, new CacheEntry(trends, Instant.now())))
                .onErrorResume(ex -> {
                    log.error("Failed to fetch Google Trends for {}: {}", geo, ex.getMessage());
                    CacheEntry stale = geoCache.get(geo);
                    return stale != null ? Mono.just(stale.trends()) : Mono.just(List.of());
                });
    }

    public Flux<List<GoogleTrend>> trendStream(String geo) {
        // computeIfAbsent ensures only one Flux.interval exists per geo regardless
        // of how many SSE clients are connected. share() ref-counts subscribers and
        // cancels the interval when the last subscriber disconnects.
        return sharedStreams.computeIfAbsent(geo, g ->
                Flux.interval(Duration.ZERO, POLL_INTERVAL)
                        .flatMap(tick -> fetchTrends(g))
                        .share()
        );
    }

    public List<GoogleTrend> getCache() {
        return geoCache.isEmpty() ? List.of()
                : geoCache.values().iterator().next().trends();
    }

    private List<GoogleTrend> parseRss(String xml) {
        List<GoogleTrend> trends = new ArrayList<>();
        Matcher items = ITEM_P.matcher(xml);

        while (items.find()) {
            String item = items.group(1);

            String title = first(TITLE_P.matcher(item));
            String traffic = first(TRAFFIC_P.matcher(item));
            String pubDate = first(PUBDATE_P.matcher(item));

            // Build search URL
            String searchUrl = title != null
                    ? "https://www.google.com/search?q=" + title.replace(" ", "+") + "&tbm=nws"
                    : "#";

            // Parse nested news items
            List<GoogleTrend.NewsArticle> articles = new ArrayList<>();
            Matcher newsM = NEWS_P.matcher(item);
            while (newsM.find()) {
                String news = newsM.group(1);
                String nTitle   = first(NEWS_TITLE_P.matcher(news));
                String nUrl     = first(NEWS_URL_P.matcher(news));
                String nSource  = first(NEWS_SOURCE_P.matcher(news));
                String nSnippet = first(NEWS_SNIPPET_P.matcher(news));
                if (nTitle != null)
                    articles.add(new GoogleTrend.NewsArticle(nTitle, nUrl, nSource, nSnippet));
            }

            if (title != null)
                trends.add(new GoogleTrend(title, searchUrl, traffic, pubDate, articles));
        }

        return trends;
    }

    /** Returns first captured group (handles CDATA and plain variants). */
    private String first(Matcher m) {
        if (!m.find()) return null;
        for (int i = 1; i <= m.groupCount(); i++) {
            String g = m.group(i);
            if (g != null && !g.isBlank()) return g.trim();
        }
        return null;
    }
}
