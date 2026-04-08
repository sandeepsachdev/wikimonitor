package com.wikipedia.monitor.model;

import java.util.List;

public record GoogleTrend(
        String title,
        String searchUrl,
        String approxTraffic,
        String pubDate,
        List<NewsArticle> articles
) {
    public record NewsArticle(String title, String url, String source, String snippet) {}
}
