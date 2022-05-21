package org.example.exercise.topn;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * 统计每一个url的访问次数并收集更新HashMap，最终把它转为ArrayList排序后输出给ProcessWindowFunction
 */
public class UrlAggFuncAll implements AggregateFunction<String, HashMap<String, Long>, ArrayList<Tuple2<String, Long>>> {
    @Override
    public HashMap<String, Long> createAccumulator() {
        return new HashMap<>();
    }

    @Override
    public HashMap<String, Long> add(String value, HashMap<String, Long> accumulator) {
        if (!accumulator.containsKey(value)) {
            accumulator.put(value, 0L);
        }
        accumulator.put(value, accumulator.get(value) + 1);
        return accumulator;
    }

    @Override
    public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> accumulator) {
        ArrayList<Tuple2<String, Long>> tuple = new ArrayList<>();
        accumulator.keySet().forEach(key -> tuple.add(Tuple2.of(key, accumulator.get(key))));
        tuple.sort((t1, t2) -> (int) (t2.f1 - t1.f1));
        return tuple;
    }

    @Override
    public HashMap<String, Long> merge(HashMap<String, Long> a, HashMap<String, Long> b) {
        return null;
    }
}
