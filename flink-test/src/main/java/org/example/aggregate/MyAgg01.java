package org.example.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.example.util.Event;

/**
 * 增量聚合函数和全窗口函数结合使用
 */
public class MyAgg01 implements AggregateFunction<Event, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Event value, Long accumulator) {
        return Long.sum(accumulator, 1);
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return Long.sum(a, b);
    }
}
