package org.example.exercise.topn;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UrlProcessFunc extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
    @Override
    public void process(String url, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
        long pv = elements.iterator().next();
        long start = context.window().getStart();
        long end = context.window().getEnd();
        out.collect(new UrlViewCount(url, pv, start, end));
    }
}
