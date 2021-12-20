package com.orderbook.rebuilder;

import com.orderbook.rebuilder.model.OrderBookSingleCurrencyEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Instant;

public class StreamingJob {

    private static final long CHECKPOINTING_INTERVAL_MS = 60000;
    private static final String JOB_NAME = "Flink Streaming Java API Application";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<OrderBookSingleCurrencyEvent> orderBookEvents =
            env
                .addSource(new OrderBookEventSource())
                .name("order-book-event-source");

        final OutputTag<Instant> eventsTimestampOutput = new OutputTag<>("side-output") {};

        SingleOutputStreamOperator<OrderBookSingleCurrencyEvent> processed =
        orderBookEvents
            .keyBy(new OrderBookSingleCurrencyEventKeySelector())
            .process(new OrderBookEventProcessFunction())
            .name("order-book-event-processor");
            /*.addSink(new DiscardingSink<>())
            .name("discarding-sink");*/

        DataStream<Long> averages = processed
            .getSideOutput(eventsTimestampOutput)
            .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(20)))
            .aggregate(new OrderBookEventProcessingTimeAggregate(), new OrderBookEventProcessingTimeWindowFunction())
            .name("order-book-event-processing-time-aggregator");

        averages
            .map(new HistogramMapFunction());
            //.addSink(new LoggingSink<>())
            //.name("discarding-sink");

        env
            //.enableCheckpointing(CHECKPOINTING_INTERVAL_MS)
            //.setStateBackend(new EmbeddedRocksDBStateBackend())
            .execute(JOB_NAME);
    }
}
