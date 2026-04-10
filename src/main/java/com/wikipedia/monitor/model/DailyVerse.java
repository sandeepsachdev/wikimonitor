package com.wikipedia.monitor.model;

import java.util.List;

public record DailyVerse(
        String reference,
        String text,
        String version,
        Insight insight,
        List<String> questions
) {
    public record Insight(String author, String work, String quote) {}
}