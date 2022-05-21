package org.example.exercise.topn;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * 将上游AggregateFunction发过来的ArrayList遍历收集到String并输出
 */
public class UrlProcessFuncAll extends ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow> {
    private final int topN;

    public UrlProcessFuncAll(int topN) {
        this.topN = topN;
    }

    @Override
    public void process(ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow>.Context context, Iterable<ArrayList<Tuple2<String, Long>>> elements, Collector<String> out) throws Exception {
        StringBuilder str = new StringBuilder();
        ArrayList<Tuple2<String, Long>> list = elements.iterator().next();
        str.append("--------------------").append("\n");
        str.append("窗口结束时间：").append(new Timestamp(context.window().getEnd())).append("\n");
        for (int i = 0; i < topN; i ++) {
            String info = "No. " + (i + 1) +  " URL: " + list.get(i).f0 + ", PV: " + list.get(i).f1;
            str.append(info).append("\n");
        }
        str.append("--------------------").append("\n");
        out.collect(str.toString());
    }
}
