package org.example.exercise.window;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * ProcessWindowFunction每个窗口结束时处理
 * 处理AggregateFunction传过来的HashSet<String>并包装输出
 */
public class UVProcessFunc extends ProcessWindowFunction<HashSet<String>, WindowResult, String, TimeWindow> {

    @Override
    public void process(String key, ProcessWindowFunction<HashSet<String>, WindowResult, String, TimeWindow>.Context context, Iterable<HashSet<String>> elements, Collector<WindowResult> out) throws Exception {
        long start = context.window().getStart();
        long end = context.window().getEnd();
        int uv = elements.iterator().next().size();
        out.collect(new WindowResult(key, uv, start, end));
    }
}
