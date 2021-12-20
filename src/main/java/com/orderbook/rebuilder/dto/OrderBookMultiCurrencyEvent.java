package com.orderbook.rebuilder.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.orderbook.rebuilder.model.Currency;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;

public class OrderBookMultiCurrencyEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("ch")
    private String channel;

    @JsonProperty("snapshot")
    private Map<Currency, OrderBookMultiCurrencyEventPayload> snapshotData;

    @JsonProperty("update")
    private Map<Currency, OrderBookMultiCurrencyEventPayload> updateData;

    private Instant createdAt;

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Map<Currency, OrderBookMultiCurrencyEventPayload> getSnapshotData() {
        return snapshotData;
    }

    public void setSnapshotData(Map<Currency, OrderBookMultiCurrencyEventPayload> snapshotData) {
        this.snapshotData = snapshotData;
    }

    public Map<Currency, OrderBookMultiCurrencyEventPayload> getUpdateData() {
        return updateData;
    }

    public void setUpdateData(Map<Currency, OrderBookMultiCurrencyEventPayload> updateData) {
        this.updateData = updateData;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public String toString() {
        return "OrderBookMultiCurrencyEvent{" +
            "channel='" + channel + '\'' +
            ", snapshotData=" + snapshotData +
            ", updateData=" + updateData +
            ", createdAt=" + createdAt +
            '}';
    }
}
