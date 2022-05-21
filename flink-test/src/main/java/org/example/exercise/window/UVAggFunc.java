package org.example.exercise.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.example.util.Event;

import java.util.HashSet;

/**
 * 每个窗口内每进一个数处理一次
 * 将相同key下的user放到HashSet<String>中传给下游ProcessWindowFunction
 */
public class UVAggFunc implements AggregateFunction<Event, HashSet<String>, HashSet<String>> {

    @Override
    public HashSet<String> createAccumulator() {
        return new HashSet<>();
    }

    @Override
    public HashSet<String> add(Event value, HashSet<String> accumulator) {
        accumulator.add(value.getUser());
        return accumulator;
    }

    @Override
    public HashSet<String> getResult(HashSet<String> accumulator) {
        return accumulator;
    }

    @Override
    public HashSet<String> merge(HashSet<String> a, HashSet<String> b) {
        return null;
    }
}
