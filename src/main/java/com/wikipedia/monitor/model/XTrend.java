package com.wikipedia.monitor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public record XTrend(
        @JsonProperty("trend_name") String trendName,
        @JsonProperty("tweet_count") Long tweetCount
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record ApiResponse(List<XTrend> data, Integer woeid) {}
}
