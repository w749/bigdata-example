package org.example.exercise.topn;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.example.util.Event;

public class UrlAggFunc implements AggregateFunction<Event, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Event value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
