package com.wikipedia.monitor.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public record WikipediaEdit(
        String type,
        String wiki,
        String title,
        String user,
        boolean bot,
        boolean minor,
        String comment,
        @JsonProperty("server_url") String serverUrl,
        @JsonProperty("server_name") String serverName,
        Long timestamp,
        Length length,
        Revision revision
) {
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Length(
            Integer old,
            @JsonProperty("new") Integer newLength
    ) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    public record Revision(
            Long old,
            @JsonProperty("new") Long newRevision
    ) {}

    public int diffSize() {
        if (length == null || length.old() == null || length.newLength() == null) return 0;
        return length.newLength() - length.old();
    }

    public String articleUrl() {
        if (serverUrl == null || title == null) return "#";
        return serverUrl + "/wiki/" + title.replace(" ", "_");
    }
}
