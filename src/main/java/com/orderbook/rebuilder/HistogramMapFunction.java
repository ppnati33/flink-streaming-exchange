package com.orderbook.rebuilder;

import com.codahale.metrics.UniformReservoir;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardHistogramWrapper;
import org.apache.flink.metrics.Histogram;

public class HistogramMapFunction extends RichMapFunction<Long, Long> {

    private transient Histogram histogram;

    @Override
    public void open(Configuration config) {
        com.codahale.metrics.Histogram dropwizardHistogram =
            new com.codahale.metrics.Histogram(new UniformReservoir(5000));

        this.histogram = getRuntimeContext()
            .getMetricGroup()
            .histogram("myHistogram", new DropwizardHistogramWrapper(dropwizardHistogram));
    }

    @Override
    public Long map(Long value) throws Exception {
        this.histogram.update(value);
        return value;
    }
}
