package com.orderbook.rebuilder.model;

import java.io.Serializable;
import java.math.BigDecimal;

public class LevelPrice implements Comparable<LevelPrice>, Serializable {

    private static final long serialVersionUID = 1L;

    private LevelPriceType type;
    private BigDecimal price;
    private BigDecimal volume;

    public LevelPrice() {
    }

    public LevelPrice(LevelPriceType type, BigDecimal price, BigDecimal volume) {
        this.type = type;
        this.price = price;
        this.volume = volume;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    public BigDecimal getVolume() {
        return volume;
    }

    public void setVolume(BigDecimal volume) {
        this.volume = volume;
    }

    public LevelPriceType getType() {
        return type;
    }

    public void setType(LevelPriceType type) {
        this.type = type;
    }

    @Override
    public int compareTo(LevelPrice levelPrice) {
        int ret;
        if (this.getType() == levelPrice.getType()) {
            ret = this.getPrice().compareTo(levelPrice.getPrice()) * (this.getType() == LevelPriceType.BID ? -1 : 1);
        } else {
            ret = this.getType() == LevelPriceType.BID ? -1 : 1;
        }

        return ret;
    }

    @Override
    public String toString() {
        return "LevelPrice{" +
            "type=" + type +
            ", price=" + price +
            ", volume=" + volume +
            '}';
    }
}
