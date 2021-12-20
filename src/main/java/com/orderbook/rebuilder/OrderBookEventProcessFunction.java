package com.orderbook.rebuilder;

import com.orderbook.rebuilder.model.OrderBook;
import com.orderbook.rebuilder.model.OrderBookSingleCurrencyEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class OrderBookEventProcessFunction
    extends KeyedProcessFunction<String, OrderBookSingleCurrencyEvent, OrderBookSingleCurrencyEvent>
    implements CheckpointedFunction {

    private static final Logger logger = LoggerFactory.getLogger(OrderBookEventProcessFunction.class.getName());

    private transient ValueState<OrderBook> orderBookState;
    private OrderBook orderBook;

    final OutputTag<Instant> eventsTimestampOutput = new OutputTag<>("side-output") {};

    @Override
    public void processElement(
        OrderBookSingleCurrencyEvent event,
        Context ctx,
        Collector<OrderBookSingleCurrencyEvent> collector
    ) throws Exception {
        try {
            //logger.info("OrderBookSingleCurrencyEvent: " + event);

            // retrieve current order book value
            orderBook = orderBookState.value();

            // initialize the state for the first time
            if (orderBook == null) {
                orderBook = new OrderBook(event.getAsks(), event.getBids(), event.getTimestamp());
            } else {
                orderBook.update(event);
            }

//            logger.info(
//                "Current order book params: asks size={}, , bids size={}",
//                orderBook.getAsks().size(),
//                orderBook.getBids().size()
//            );

            // write state back
            orderBookState.update(orderBook);

            // emit event data to regular output
            collector.collect(event);
            // emit timestamp data to side output
            ctx.output(eventsTimestampOutput, Instant.now());
        } catch (Exception e) {
            String errorMessage =
                "Unable to process OrderBookEvent: " + event.toString() +
                    ". Error: " + e.getMessage();
            logger.error(errorMessage, e);
            throw e;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
        orderBookState.clear();
        orderBookState.update(orderBook);
    }

    @Override
    public void initializeState(FunctionInitializationContext ctx) throws Exception {
        orderBookState = ctx.getKeyedStateStore().getState(
            new ValueStateDescriptor<>("orderBook",TypeInformation.of(new TypeHint<>() {}))
        );
    }
}
