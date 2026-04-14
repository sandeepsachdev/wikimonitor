package com.wikipedia.monitor.service;

import com.wikipedia.monitor.model.NewsItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class NewsAggregatorService {

    private static final Logger log = LoggerFactory.getLogger(NewsAggregatorService.class);
    private static final int CACHE_SIZE  = 300;
    private static final int DEDUP_SIZE  = 5000;
    private static final Duration POLL_INTERVAL = Duration.ofMinutes(5);

    record FeedConfig(String url, String name, String category) {}

    private static final List<FeedConfig> FEEDS = List.of(
        // World / General
        new FeedConfig("https://feeds.bbci.co.uk/news/world/rss.xml",                    "BBC World",       "World"),
        new FeedConfig("https://feeds.npr.org/1001/rss.xml",                              "NPR",             "World"),
        new FeedConfig("https://www.aljazeera.com/xml/rss/all.xml",                       "Al Jazeera",      "World"),
        new FeedConfig("https://www.theguardian.com/world/rss",                           "The Guardian",    "World"),
        new FeedConfig("https://rss.nytimes.com/services/xml/rss/nyt/World.xml",          "NY Times",        "World"),
        new FeedConfig("https://feeds.foxnews.com/foxnews/latest",                        "Fox News",        "World"),
        new FeedConfig("https://www.spiegel.de/international/index.rss",                  "Der Spiegel",     "World"),
        new FeedConfig("https://www.reddit.com/r/worldnews/.rss",                         "r/worldnews",     "World"),
        // Politics / Business
        new FeedConfig("https://www.politico.com/rss/politicopicks.xml",                  "Politico",        "Politics"),
        new FeedConfig("https://www.cnbc.com/id/100003114/device/rss/rss.html",           "CNBC",            "Business"),
        new FeedConfig("https://feeds.bbci.co.uk/news/business/rss.xml",                  "BBC Business",    "Business"),
        new FeedConfig("https://www.theguardian.com/us-news/rss",                         "Guardian US",     "Politics"),
        // Tech
        new FeedConfig("https://techcrunch.com/feed/",                                    "TechCrunch",      "Tech"),
        new FeedConfig("https://feeds.arstechnica.com/arstechnica/index",                 "Ars Technica",    "Tech"),
        new FeedConfig("https://www.theverge.com/rss/index.xml",                          "The Verge",       "Tech"),
        new FeedConfig("https://www.wired.com/feed/rss",                                  "Wired",           "Tech"),
        new FeedConfig("https://hnrss.org/frontpage",                                     "Hacker News",     "Tech"),
        new FeedConfig("https://www.reddit.com/r/technology/.rss",                        "r/technology",    "Tech"),
        new FeedConfig("https://feeds.bbci.co.uk/news/technology/rss.xml",                "BBC Tech",        "Tech"),
        // Science
        new FeedConfig("https://www.nasa.gov/rss/dyn/breaking_news.rss",                  "NASA",            "Science"),
        new FeedConfig("https://www.sciencedaily.com/rss/all.xml",                        "Science Daily",   "Science"),
        new FeedConfig("https://www.newscientist.com/feed/home/",                         "New Scientist",   "Science"),
        // Sports
        new FeedConfig("https://www.espn.com/espn/rss/news",                              "ESPN",            "Sports"),
        new FeedConfig("https://www.theguardian.com/sport/rss",                           "Guardian Sport",  "Sports")
    );

    // RSS 2.0
    private static final Pattern RSS_ITEM   = Pattern.compile("<item[^>]*>(.*?)</item>",   Pattern.DOTALL);
    // Atom (Reddit, some others)
    private static final Pattern ATOM_ENTRY = Pattern.compile("<entry[^>]*>(.*?)</entry>", Pattern.DOTALL);

    private static final Pattern TITLE       = Pattern.compile("<title[^>]*>(?:<!\\[CDATA\\[)?(.*?)(?:]]>)?</title>", Pattern.DOTALL);
    private static final Pattern LINK_HREF   = Pattern.compile("<link[^>]+href=[\"']([^\"']+)[\"']");
    private static final Pattern LINK_TEXT   = Pattern.compile("<link[^>]*>(https?://[^<]+)</link>");
    private static final Pattern PUBDATE     = Pattern.compile("<(?:pubDate|updated|dc:date)[^>]*>(.*?)</(?:pubDate|updated|dc:date)>", Pattern.DOTALL);
    private static final Pattern DESCRIPTION = Pattern.compile("<(?:description|summary)[^>]*>(?:<!\\[CDATA\\[)?(.*?)(?:]]>)?</(?:description|summary)>", Pattern.DOTALL);
    private static final Pattern HTML_TAG      = Pattern.compile("<[^>]+>");
    private static final Pattern NUMERIC_ENTITY = Pattern.compile("&#([xX][0-9a-fA-F]+|[0-9]+);");

    private final WebClient webClient;
    private final Sinks.Many<NewsItem> sink;
    private final Flux<NewsItem> sharedFlux;

    // Bounded cache for replay on new SSE connections
    private final ConcurrentLinkedDeque<NewsItem> cache = new ConcurrentLinkedDeque<>();
    // Dedup set — tracks seen links (bounded to DEDUP_SIZE)
    private final LinkedHashSet<String> seen = new LinkedHashSet<>();

    public NewsAggregatorService() {
        this.webClient = WebClient.builder()
                .defaultHeader("User-Agent", "Mozilla/5.0 (compatible; NewsBot/1.0)")
                .codecs(c -> c.defaultCodecs().maxInMemorySize(2 * 1024 * 1024))
                .build();
        this.sink = Sinks.many().multicast().onBackpressureBuffer(1000, false);
        this.sharedFlux = sink.asFlux().share();

        startPolling();
    }

    private void startPolling() {
        for (int i = 0; i < FEEDS.size(); i++) {
            final FeedConfig feed = FEEDS.get(i);
            final Duration initialDelay = Duration.ofSeconds(i * 8L); // stagger startup
            Flux.interval(initialDelay, POLL_INTERVAL)
                    .flatMap(tick -> fetchFeed(feed))
                    .subscribe(
                            item -> {
                                boolean isNew;
                                synchronized (seen) {
                                    isNew = seen.add(item.link());
                                    if (seen.size() > DEDUP_SIZE) {
                                        seen.remove(seen.iterator().next());
                                    }
                                }
                                if (isNew) {
                                    cache.addFirst(item);
                                    while (cache.size() > CACHE_SIZE) cache.pollLast();
                                    sink.tryEmitNext(item);
                                }
                            },
                            err -> log.warn("Error polling {}: {}", feed.name(), err.getMessage())
                    );
        }
    }

    private Flux<NewsItem> fetchFeed(FeedConfig feed) {
        return webClient.get()
                .uri(feed.url())
                .retrieve()
                .bodyToMono(String.class)
                .flatMapMany(xml -> Flux.fromIterable(parseItems(xml, feed)))
                .onErrorResume(ex -> {
                    log.debug("Failed to fetch {}: {}", feed.name(), ex.getMessage());
                    return Flux.empty();
                });
    }

    private List<NewsItem> parseItems(String xml, FeedConfig feed) {
        List<NewsItem> items = new ArrayList<>();

        // Try RSS <item> elements first, fall back to Atom <entry>
        Pattern blockPattern = findBlocks(xml);
        Matcher blocks = blockPattern.matcher(xml);

        while (blocks.find()) {
            String block = blocks.group(1);

            String title = extract(TITLE, block);
            if (title == null || title.isBlank()) continue;
            title = decodeEntities(title.trim());

            String link = extractLink(block);
            if (link == null || link.isBlank()) continue;
            link = link.trim();

            String pubDate = extract(PUBDATE, block);
            String rawDesc = extract(DESCRIPTION, block);
            String description = rawDesc != null
                    ? decodeEntities(HTML_TAG.matcher(decodeEntities(rawDesc)).replaceAll("").trim()
                              .replaceAll("\\s+", " "))
                    : "";
            if (description.length() > 220) description = description.substring(0, 217) + "…";

            items.add(new NewsItem(title, link, feed.name(), feed.category(), pubDate, description));
        }

        return items;
    }

    private Pattern findBlocks(String xml) {
        return xml.contains("<entry") ? ATOM_ENTRY : RSS_ITEM;
    }

    private String extractLink(String block) {
        // Try <link href="..."> (Atom)
        Matcher m = LINK_HREF.matcher(block);
        if (m.find()) return m.group(1);
        // Try <link>url</link> (RSS)
        m = LINK_TEXT.matcher(block);
        if (m.find()) return m.group(1);
        return null;
    }

    private String extract(Pattern p, String text) {
        Matcher m = p.matcher(text);
        if (!m.find()) return null;
        for (int i = 1; i <= m.groupCount(); i++) {
            String g = m.group(i);
            if (g != null && !g.isBlank()) return g.trim();
        }
        return null;
    }

    private String decodeEntities(String s) {
        // Named entities — common in news feeds
        s = s.replace("&amp;",    "&")
             .replace("&lt;",     "<")
             .replace("&gt;",     ">")
             .replace("&quot;",   "\"")
             .replace("&apos;",   "'")
             .replace("&#39;",    "'")
             .replace("&nbsp;",   " ")
             .replace("&rsquo;",  "\u2019")
             .replace("&lsquo;",  "\u2018")
             .replace("&rdquo;",  "\u201D")
             .replace("&ldquo;",  "\u201C")
             .replace("&mdash;",  "\u2014")
             .replace("&ndash;",  "\u2013")
             .replace("&hellip;", "\u2026")
             .replace("&bull;",   "\u2022")
             .replace("&middot;", "\u00B7")
             .replace("&copy;",   "\u00A9")
             .replace("&reg;",    "\u00AE")
             .replace("&trade;",  "\u2122");

        // Numeric entities — &#8217; (decimal) and &#x2019; (hex)
        Matcher m = NUMERIC_ENTITY.matcher(s);
        if (!m.find()) return s;
        StringBuffer sb = new StringBuffer();
        m.reset();
        while (m.find()) {
            String ref = m.group(1);
            try {
                int code = ref.startsWith("x") || ref.startsWith("X")
                        ? Integer.parseInt(ref.substring(1), 16)
                        : Integer.parseInt(ref, 10);
                m.appendReplacement(sb, Matcher.quoteReplacement(new String(Character.toChars(code))));
            } catch (NumberFormatException e) {
                m.appendReplacement(sb, Matcher.quoteReplacement(m.group(0)));
            }
        }
        m.appendTail(sb);
        return sb.toString();
    }

    public Flux<NewsItem> getNewsStream() {
        List<NewsItem> snapshot = new ArrayList<>(cache);
        return Flux.fromIterable(snapshot).concatWith(sharedFlux);
    }

    public List<NewsItem> getCache() {
        return new ArrayList<>(cache);
    }
}
