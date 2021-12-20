package com.orderbook.rebuilder.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;

public class OrderBookSingleCurrencyEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    Currency currency;
    List<LevelPrice> asks;
    List<LevelPrice> bids;
    Instant timestamp;
    Instant createdAt;

    public OrderBookSingleCurrencyEvent() {
    }

    public OrderBookSingleCurrencyEvent(
        Currency currency,
        List<LevelPrice> asks,
        List<LevelPrice> bids,
        Instant timestamp,
        Instant createdAt
    ) {
        this.currency = currency;
        this.asks = asks;
        this.bids = bids;
        this.timestamp = timestamp;
        this.createdAt = createdAt;
    }

    public Currency getCurrency() {
        return currency;
    }

    public void setCurrency(Currency currency) {
        this.currency = currency;
    }

    public List<LevelPrice> getAsks() {
        return asks;
    }

    public void setAsks(List<LevelPrice> asks) {
        this.asks = asks;
    }

    public List<LevelPrice> getBids() {
        return bids;
    }

    public void setBids(List<LevelPrice> bids) {
        this.bids = bids;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public String toString() {
        return "OrderBookSingleCurrencyEvent{" +
            "currency=" + currency +
            ", asks=" + asks +
            ", bids=" + bids +
            ", timestamp=" + timestamp +
            ", createdAt=" + createdAt +
            '}';
    }
}
