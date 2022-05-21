package org.example.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.example.util.Event;
import scala.Tuple2;

/**
 * 自定义AggregateFunction完成对Event内的属性求平均值
 */
public class MyAgg implements AggregateFunction<Event, Tuple2<Long, Long>, Double> {
    Tuple2<Long, Long> tuple = new Tuple2<>(0L, 0L);

    @Override
    public Tuple2<Long, Long> createAccumulator() {
        return tuple.copy(0L, 0L);
    }

    @Override
    public Tuple2<Long, Long> add(Event value, Tuple2<Long, Long> accumulator) {
        return accumulator.copy(accumulator._1 + value.getTime(), accumulator._2 + 1);
    }

    @Override
    public Double getResult(Tuple2<Long, Long> accumulator) {
        return (double) (accumulator._1 / accumulator._2);
    }

    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
        return null;
    }
}
