package com.wikipedia.monitor.model;

import java.util.List;
import java.util.Map;

public record TrendsSnapshot(
        List<MinuteBucket> editRate,
        List<RankedItem> topArticles,
        List<RankedItem> topEditors,
        Map<String, Long> wikiBreakdown,
        long totalEdits,
        long botEdits,
        long newPages,
        double editsPerMinute
) {
    public record MinuteBucket(String label, long edits, long bots, long newPages) {}
    public record RankedItem(String name, long count, String url) {}
}
