package com.orderbook.rebuilder.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

public class OrderBookMultiCurrencyEventPayload implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("t")
    private Long timestamp;

    @JsonProperty("s")
    private Long sequenceNum;

    @JsonProperty("a")
    private List<List<BigDecimal>> asks;

    @JsonProperty("b")
    private List<List<BigDecimal>> bids;

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getSequenceNum() {
        return sequenceNum;
    }

    public void setSequenceNum(Long sequenceNum) {
        this.sequenceNum = sequenceNum;
    }

    public List<List<BigDecimal>> getAsks() {
        return asks;
    }

    public void setAsks(List<List<BigDecimal>> asks) {
        this.asks = asks;
    }

    public List<List<BigDecimal>> getBids() {
        return bids;
    }

    public void setBids(List<List<BigDecimal>> bids) {
        this.bids = bids;
    }

    @Override
    public String toString() {
        return "OrderBookMultiCurrencyEventPayload{" +
            "timestamp=" + timestamp +
            ", sequenceNum=" + sequenceNum +
            ", asks=" + asks +
            ", bids=" + bids +
            '}';
    }
}
