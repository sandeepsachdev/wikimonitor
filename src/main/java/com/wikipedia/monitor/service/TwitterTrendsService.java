package com.wikipedia.monitor.service;

import com.wikipedia.monitor.model.TwitterTrend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class TwitterTrendsService {

    private static final Logger log = LoggerFactory.getLogger(TwitterTrendsService.class);
    private static final Duration CACHE_TTL = Duration.ofMinutes(15);

    private record CacheEntry(List<TwitterTrend> trends, Instant fetchedAt) {}

    private final WebClient webClient;
    private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();

    // Match the first <ol> with "trend" in the class attribute (trends24.in uses trend-card-list),
    // falling back to any <ol> if no class matches.
    private static final Pattern TREND_OL = Pattern.compile(
            "<ol[^>]*class=\"[^\"]*trend[^\"]*\"[^>]*>(.*?)</ol>",
            Pattern.DOTALL | Pattern.CASE_INSENSITIVE
    );
    private static final Pattern ANY_OL = Pattern.compile(
            "<ol[^>]*>(.*?)</ol>", Pattern.DOTALL
    );
    private static final Pattern LI_BLOCK = Pattern.compile(
            "<li[^>]*>(.*?)</li>", Pattern.DOTALL
    );
    private static final Pattern ANCHOR = Pattern.compile(
            "<a[^>]*>\\s*([^<\\s][^<]{0,80}?)\\s*</a>"
    );
    // Matches counts like "1,234", "12.5K", "1.2M"
    private static final Pattern COUNT = Pattern.compile(
            "([\\d][\\d,]*(?:\\.[\\d]+)?\\s*[KkMm]?)\\s*\\+?"
    );

    public TwitterTrendsService() {
        this.webClient = WebClient.builder()
                .defaultHeader("User-Agent",
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " +
                        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
                .defaultHeader("Accept-Language", "en-US,en;q=0.9")
                .codecs(c -> c.defaultCodecs().maxInMemorySize(4 * 1024 * 1024))
                .build();
    }

    public Mono<List<TwitterTrend>> getTrends(String country) {
        CacheEntry entry = cache.get(country);
        if (entry != null &&
                Duration.between(entry.fetchedAt(), Instant.now()).compareTo(CACHE_TTL) < 0) {
            return Mono.just(entry.trends());
        }

        String slug = countrySlug(country);
        String url = "https://trends24.in/" + (slug.isEmpty() ? "" : slug + "/");

        return webClient.get()
                .uri(url)
                .retrieve()
                .bodyToMono(String.class)
                .map(this::parseHtml)
                .doOnNext(trends -> cache.put(country, new CacheEntry(trends, Instant.now())))
                .onErrorResume(ex -> {
                    log.warn("Failed to fetch trends for {}: {}", country, ex.getMessage());
                    // Serve stale cache rather than an empty list
                    CacheEntry stale = cache.get(country);
                    return stale != null ? Mono.just(stale.trends()) : Mono.just(List.of());
                });
    }

    private List<TwitterTrend> parseHtml(String html) {
        List<TwitterTrend> trends = new ArrayList<>();

        // Try class-targeted pattern first, fall back to any <ol>
        Matcher olMatcher = TREND_OL.matcher(html);
        String listContent = olMatcher.find() ? olMatcher.group(1) : null;

        if (listContent == null) {
            olMatcher = ANY_OL.matcher(html);
            if (!olMatcher.find()) return trends;
            listContent = olMatcher.group(1);
        }

        Matcher liMatcher = LI_BLOCK.matcher(listContent);
        while (liMatcher.find() && trends.size() < 20) {
            String li = liMatcher.group(1);

            Matcher anchor = ANCHOR.matcher(li);
            if (!anchor.find()) continue;
            String name = anchor.group(1).trim();
            if (name.isEmpty() || name.length() > 100) continue;

            // Strip tags and look for a count value after the trend name
            String plainText = li.replaceAll("<[^>]+>", " ")
                                 .replaceAll("\\s+", " ").trim();
            String postCount = extractCount(plainText, name);

            String searchUrl = "https://x.com/search?q="
                    + URLEncoder.encode(name, StandardCharsets.UTF_8)
                    + "&src=trend_click&f=live";

            trends.add(new TwitterTrend(name, searchUrl, postCount));
        }

        return trends;
    }

    private String extractCount(String plainText, String trendName) {
        // Remove the trend name itself so we don't match digits inside it
        String remainder = plainText.replace(trendName, "").trim();
        Matcher m = COUNT.matcher(remainder);
        while (m.find()) {
            String candidate = m.group(1).replaceAll("\\s", "");
            // Ignore single digits or the number "1" on its own (rank numbers etc.)
            if (candidate.length() > 1 || candidate.matches("[2-9]")) {
                return candidate;
            }
        }
        return null;
    }

    private String countrySlug(String country) {
        return switch (country.toLowerCase()) {
            case "worldwide", ""           -> "";
            case "united-states", "us"     -> "united-states";
            case "united-kingdom", "uk"    -> "united-kingdom";
            case "australia",      "au"    -> "australia";
            case "canada",         "ca"    -> "canada";
            case "india",          "in"    -> "india";
            case "germany",        "de"    -> "germany";
            case "france",         "fr"    -> "france";
            case "japan",          "jp"    -> "japan";
            case "brazil",         "br"    -> "brazil";
            case "mexico",         "mx"    -> "mexico";
            case "south-korea",    "kr"    -> "south-korea";
            case "spain",          "es"    -> "spain";
            case "italy",          "it"    -> "italy";
            case "indonesia",      "id"    -> "indonesia";
            case "argentina",      "ar"    -> "argentina";
            case "turkey",         "tr"    -> "turkey";
            case "nigeria",        "ng"    -> "nigeria";
            case "south-africa",   "za"    -> "south-africa";
            default -> country.toLowerCase().replace(" ", "-");
        };
    }
}
