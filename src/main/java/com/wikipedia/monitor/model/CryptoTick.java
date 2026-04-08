package com.wikipedia.monitor.model;

public record CryptoTick(
        String symbol,
        double price,
        double open,
        double high,
        double low,
        double volume
) {
    public double changePercent() {
        return open == 0 ? 0 : (price - open) / open * 100;
    }
}
