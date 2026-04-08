package com.wikipedia.monitor.model;

public record NewsItem(
        String title,
        String link,
        String source,
        String category,
        String pubDate,
        String description
) {}
