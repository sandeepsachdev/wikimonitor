# WikipediaMonitor

A real-time dashboard that streams live Wikipedia edits, Wikipedia trending topics, Google Trends, X (Twitter) trends, and Bluesky posts — all in a single Spring Boot application.

## Features

- **Live Wikipedia edits feed** — streams from `stream.wikimedia.org` via Server-Sent Events (SSE), filterable by wiki/language
- **Wikipedia trending topics** — aggregates the most-edited articles in real time
- **Google Trends** — polls the Google Trends RSS feed every 15 minutes per region
- **Bluesky firehose** — connects to the Bluesky Jetstream WebSocket and streams live posts

## Tech Stack

- **Java 21** + **Spring Boot 3.2** (WebFlux / reactive)
- **Project Reactor** — Flux/Sink for streaming pipelines
- **Lombok** — boilerplate reduction
- **Vanilla HTML/CSS/JS** — no frontend framework; SSE consumed natively in the browser
- **Docker** — multi-stage build (Maven → JRE)

## Running Locally

### Prerequisites

- Java 21+
- Maven 3.9+ (or use the included `mvnw` wrapper)

```bash
./mvnw spring-boot:run
```

Open `http://localhost:8080` in your browser.

### With Docker

```bash
docker build -t wikipedia-monitor .
docker run -p 8080:8080 wikipedia-monitor
```

## Pages

| Path | Description |
|------|-------------|
| `/` | Live Wikipedia edits feed |
| `/trends.html` | Wikipedia trending topics |
| `/google-trends.html` | Google Trends (by region) |
| `/bluesky.html` | Live Bluesky posts |

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /stream/edits` | SSE stream of all Wikipedia edits |
| `GET /stream/edits?wiki=enwiki` | SSE stream filtered to a specific wiki |
| `GET /api/trends` | Wikipedia trending articles (SSE) |
| `GET /api/google-trends` | Google Trends list (SSE, by `?geo=US`) |
| `GET /api/bluesky` | Bluesky post stream (SSE) |

---

## Prompts Used to Build This Project

The following prompts were used with Claude Code to build this project from scratch.

### 1. Project Scaffold

```
Create a Spring Boot 3.2 project using WebFlux (reactive) with Java 21, Lombok, and Jackson.
Group ID: com.wikipedia, Artifact ID: monitor. Include a Dockerfile with a multi-stage build
(Maven build stage → JRE runtime stage). Include application.properties with server.port=8080.
```

### 2. Wikipedia Live Edit Stream

```
Create a WikipediaStreamService that connects to https://stream.wikimedia.org/v2/stream/recentchange
using Spring WebFlux WebClient. Stream Server-Sent Events, parse each JSON payload into a
WikipediaEdit record (fields: type, wiki, title, user, bot, minor, comment, server_url, timestamp,
length with old/new). Expose a shared Flux<WikipediaEdit> via a Reactor Sinks.Many multicast sink
with a buffer of 1000. Add retry with exponential backoff up to 30 seconds.
```

### 3. Wikipedia SSE Controller

```
Create a WikipediaStreamController REST controller with a GET /stream/edits endpoint that
produces text/event-stream. Accept an optional ?wiki= query parameter to filter by wiki name.
Wrap each WikipediaEdit in a ServerSentEvent with an incrementing ID and event type "edit".
```

### 4. Live Feed Frontend

```
Create a static index.html (dark GitHub-style theme, #0d1117 background) that connects to
/stream/edits using the browser EventSource API. Display each edit as a card showing: wiki badge,
article title (linked to the article), edit type tags (NEW/BOT/MINOR), username, timestamp, and
byte diff (green for additions, red for deletions). Add controls: wiki language dropdown filter,
hide-bots checkbox, pause/resume button, clear button. Show a stats bar with total edits,
edits/min, new pages, bot edits, and connection status. Cap display at 200 cards, auto-reconnect
on disconnect. Use slide-in animation for new cards.
```

### 5. Wikipedia Trending Topics

```
Create a TrendsService that subscribes to the WikipediaStreamService shared Flux and maintains
an in-memory map of article edit counts over the last 10 minutes (sliding window). Expose a
TrendsController with a GET /api/trends SSE endpoint that emits the top 20 articles every
10 seconds as a JSON array. Create trends.html that displays these as a live leaderboard with
rank, article name, edit count, and a link to the Wikipedia article.
```

### 6. Google Trends Integration

```
Create a GoogleTrendsService that polls https://trends.google.com/trending/rss?geo=US every
15 minutes using WebFlux WebClient. Parse the RSS XML with regex (no XML library) to extract
trend title, approximate traffic, pub date, and nested news articles (title, URL, source,
snippet). Handle CDATA sections. Expose a GoogleTrendsController with a GET /api/google-trends
SSE endpoint (accepts ?geo= param). Create google-trends.html showing trending topics as cards
with expandable news articles.
```

### 7. Bluesky Firehose Integration

```
Create a BlueskyFirehoseService that connects to the Bluesky Jetstream WebSocket at
wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post
using ReactorNettyWebSocketClient. Parse each JSON message into a BlueskyPost record (did,
time_us, kind, commit with operation/collection/rkey, and nested record with text, langs, createdAt).
Filter to only "create" operations. Expose a shared Flux via a multicast Sink with buffer 2000.
Add exponential-backoff reconnection. Create a BlueskyController with a GET /api/bluesky SSE
endpoint. Create bluesky.html that shows live posts with author DID, post text, language tags,
and timestamp.
```

### 8. Navigation Bar

```
Add a navigation bar to all HTML pages (index.html, trends.html, google-trends.html,
bluesky.html) with links to each page. Style the active page with a highlighted border.
Use the same dark theme as the rest of the UI.
```

### 9. Docker Support

```
Update the Dockerfile to use a multi-stage build: first stage uses maven:3.9-eclipse-temurin-17
to build the jar with mvn package -DskipTests, second stage uses eclipse-temurin:17-jre to run
the jar. Expose port 8080. Pre-fetch dependencies in a separate layer for better caching.
```