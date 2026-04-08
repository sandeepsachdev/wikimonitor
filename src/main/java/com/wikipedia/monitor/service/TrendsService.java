package com.wikipedia.monitor.service;

import com.wikipedia.monitor.model.TrendsSnapshot;
import com.wikipedia.monitor.model.TrendsSnapshot.MinuteBucket;
import com.wikipedia.monitor.model.TrendsSnapshot.RankedItem;
import com.wikipedia.monitor.model.WikipediaEdit;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

@Service
public class TrendsService {

    private static final int WINDOW_MINUTES = 15;
    private static final DateTimeFormatter BUCKET_FMT =
            DateTimeFormatter.ofPattern("HH:mm").withZone(ZoneOffset.UTC);

    // Keyed by minute epoch (System.currentTimeMillis() / 60_000)
    private final ConcurrentHashMap<Long, BucketData> minuteBuckets = new ConcurrentHashMap<>();

    // Rolling counters for the full window
    private final ConcurrentHashMap<String, LongAdder> articleCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, String> articleUrls = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> editorCounts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, LongAdder> wikiCounts = new ConcurrentHashMap<>();

    private final LongAdder totalEdits = new LongAdder();
    private final LongAdder totalBots = new LongAdder();
    private final LongAdder totalNew = new LongAdder();

    public TrendsService(WikipediaStreamService streamService) {
        streamService.getEditStream().subscribe(this::record);
    }

    private void record(WikipediaEdit edit) {
        long bucketKey = System.currentTimeMillis() / 60_000;
        BucketData bucket = minuteBuckets.computeIfAbsent(bucketKey, k -> new BucketData());
        bucket.edits.increment();
        if (edit.bot()) bucket.bots.increment();
        if ("new".equals(edit.type())) bucket.newPages.increment();

        if (edit.title() != null) {
            articleCounts.computeIfAbsent(edit.title(), k -> new LongAdder()).increment();
            if (edit.serverUrl() != null)
                articleUrls.putIfAbsent(edit.title(), edit.articleUrl());
        }
        if (edit.user() != null && !edit.bot())
            editorCounts.computeIfAbsent(edit.user(), k -> new LongAdder()).increment();
        if (edit.wiki() != null)
            wikiCounts.computeIfAbsent(edit.wiki(), k -> new LongAdder()).increment();

        totalEdits.increment();
        if (edit.bot()) totalBots.increment();
        if ("new".equals(edit.type())) totalNew.increment();

        evictOldBuckets();
    }

    private void evictOldBuckets() {
        long cutoff = System.currentTimeMillis() / 60_000 - WINDOW_MINUTES;
        minuteBuckets.keySet().removeIf(k -> k < cutoff);
    }

    public TrendsSnapshot snapshot() {
        evictOldBuckets();

        long now = System.currentTimeMillis() / 60_000;
        List<MinuteBucket> buckets = new ArrayList<>();
        for (int i = WINDOW_MINUTES - 1; i >= 0; i--) {
            long key = now - i;
            BucketData data = minuteBuckets.getOrDefault(key, BucketData.EMPTY);
            String label = BUCKET_FMT.format(Instant.ofEpochSecond(key * 60));
            buckets.add(new MinuteBucket(label, data.edits.sum(), data.bots.sum(), data.newPages.sum()));
        }

        List<RankedItem> topArticles = articleCounts.entrySet().stream()
                .sorted(Map.Entry.<String, LongAdder>comparingByValue(
                        Comparator.comparingLong(LongAdder::sum)).reversed())
                .limit(10)
                .map(e -> new RankedItem(e.getKey(), e.getValue().sum(),
                        articleUrls.getOrDefault(e.getKey(), "#")))
                .collect(Collectors.toList());

        List<RankedItem> topEditors = editorCounts.entrySet().stream()
                .sorted(Map.Entry.<String, LongAdder>comparingByValue(
                        Comparator.comparingLong(LongAdder::sum)).reversed())
                .limit(10)
                .map(e -> new RankedItem(e.getKey(), e.getValue().sum(), ""))
                .collect(Collectors.toList());

        Map<String, Long> wikiBreakdown = new LinkedHashMap<>();
        wikiCounts.entrySet().stream()
                .sorted(Map.Entry.<String, LongAdder>comparingByValue(
                        Comparator.comparingLong(LongAdder::sum)).reversed())
                .limit(10)
                .forEach(e -> wikiBreakdown.put(e.getKey(), e.getValue().sum()));

        long recentEdits = buckets.subList(Math.max(0, buckets.size() - 5), buckets.size())
                .stream().mapToLong(MinuteBucket::edits).sum();
        double rate = recentEdits / 5.0;

        return new TrendsSnapshot(buckets, topArticles, topEditors, wikiBreakdown,
                totalEdits.sum(), totalBots.sum(), totalNew.sum(), rate);
    }

    private static class BucketData {
        final LongAdder edits = new LongAdder();
        final LongAdder bots = new LongAdder();
        final LongAdder newPages = new LongAdder();
        static final BucketData EMPTY = new BucketData();
    }
}
