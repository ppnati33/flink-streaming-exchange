package com.orderbook.rebuilder.model;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.orderbook.rebuilder.model.LevelPriceType.ASK;
import static com.orderbook.rebuilder.model.LevelPriceType.BID;

public class OrderBook implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<LevelPrice> asks;
    private List<LevelPrice> bids;
    private Instant updatedAt;

    public OrderBook() {
    }

    public OrderBook(
        List<LevelPrice> asks,
        List<LevelPrice> bids,
        Instant updatedAt
    ) {
        this.asks = asks.stream().sorted().collect(Collectors.toList());
        this.bids = bids.stream().sorted().collect(Collectors.toList());
        this.updatedAt = updatedAt;
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

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Instant updatedAt) {
        this.updatedAt = updatedAt;
    }

    public void update(OrderBookSingleCurrencyEvent updateData) {
        updateData.getAsks().forEach(askToUpdate -> {
            int idx = Collections.binarySearch(asks, askToUpdate);

            if (idx >= 0) {
                asks.remove(idx);
            } else {
                idx = -idx - 1;
            }

            if (askToUpdate.getVolume().compareTo(BigDecimal.ZERO) != 0) {
                LevelPrice updated = withAmount(askToUpdate, askToUpdate.getVolume());
                asks.add(idx, updated);
            }

            Collections.sort(asks);
        });

        updateData.getBids().forEach(bidToUpdate -> {
            int idx = Collections.binarySearch(bids, bidToUpdate);

            if (idx >= 0) {
                bids.remove(idx);
            } else {
                idx = -idx - 1;
            }

            if (bidToUpdate.getVolume().compareTo(BigDecimal.ZERO) != 0) {
                LevelPrice updated = withAmount(bidToUpdate, bidToUpdate.getVolume());
                bids.add(idx, updated);
            }

            Collections.sort(bids);
        });

        updateTimestamp(updateData.getTimestamp());
    }

    private List<LevelPrice> getLevelPricesByType(LevelPriceType type) {
        if (type == ASK) return asks;
        if (type == BID) return bids;
        throw new IllegalArgumentException("LevelPriceType " + type + " is not supported!");
    }

    private LevelPrice withAmount(LevelPrice levelPrice, BigDecimal tradeableAmount) {
        LevelPriceType type = levelPrice.getType();
        BigDecimal price = levelPrice.getPrice();
        return new LevelPrice(type, price, tradeableAmount);
    }

    private void updateTimestamp(Instant updateTimestamp) {
        if (updateTimestamp != null) {
            if (this.updatedAt == null || updateTimestamp.isAfter(this.updatedAt)) {
                this.updatedAt = updateTimestamp;
            }
        }
    }

    @Override
    public String toString() {
        return "OrderBook{" +
            "asks=" + asks +
            ", bids=" + bids +
            ", updatedAt=" + updatedAt +
            '}';
    }
}
