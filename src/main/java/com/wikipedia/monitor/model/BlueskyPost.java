package com.wikipedia.monitor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Represents a post event from the Bluesky Jetstream WebSocket firehose.
 * Jetstream docs: https://github.com/bluesky-social/jetstream
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record BlueskyPost(
        String did,
        @JsonProperty("time_us") Long timeUs,
        String kind,
        Commit commit
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Commit(
            String operation,
            String collection,
            String rkey,
            PostRecord record
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record PostRecord(
            String text,
            String createdAt,
            List<String> langs,
            Embed embed
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Embed(
            @JsonProperty("$type") String type,
            List<Image> images,
            External external
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Image(Alt alt) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Alt(String alt) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record External(String uri, String title, String description) {}

    // ── Derived helpers ──────────────────────────────────────────────────

    public boolean isCreate() {
        return commit != null && "create".equals(commit.operation())
                && "app.bsky.feed.post".equals(commit.collection());
    }

    public String text() {
        return commit != null && commit.record() != null ? commit.record().text() : null;
    }

    public String createdAt() {
        return commit != null && commit.record() != null ? commit.record().createdAt() : null;
    }

    public List<String> langs() {
        return commit != null && commit.record() != null ? commit.record().langs() : null;
    }

    public String postUrl() {
        if (did == null || commit == null || commit.rkey() == null) return null;
        return "https://bsky.app/profile/" + did + "/post/" + commit.rkey();
    }

    public String profileUrl() {
        return did != null ? "https://bsky.app/profile/" + did : null;
    }

    public boolean hasImage() {
        if (commit == null || commit.record() == null || commit.record().embed() == null) return false;
        String type = commit.record().embed().type();
        return type != null && type.contains("images");
    }

    public boolean hasLink() {
        if (commit == null || commit.record() == null || commit.record().embed() == null) return false;
        String type = commit.record().embed().type();
        return type != null && type.contains("external");
    }
}
