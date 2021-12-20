package com.orderbook.rebuilder;

import com.orderbook.rebuilder.model.OrderBookSingleCurrencyEvent;
import org.apache.flink.api.java.functions.KeySelector;

public class OrderBookSingleCurrencyEventKeySelector
    implements KeySelector<OrderBookSingleCurrencyEvent, String> {

    @Override
    public String getKey(OrderBookSingleCurrencyEvent orderBookSingleCurrencyEvent) {
        return orderBookSingleCurrencyEvent.getCurrency().name();
    }
}
