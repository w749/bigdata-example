package org.example.aggregate;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.sql.Timestamp;

/**
 * 增量聚合函数和全窗口函数结合使用
 * 这里的输入就是MyAgg01的输出，因为是窗口结束后才调用，所以只有一个结果
 */
public class MyProcess01 extends ProcessWindowFunction<Long, String, String, TimeWindow> {

    @Override
    public void process(String key, ProcessWindowFunction<Long, String, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<String> out) throws Exception {
        Timestamp start = new Timestamp(context.window().getStart());
        Timestamp end = new Timestamp(context.window().getEnd());
        Long sum = elements.iterator().next();
        out.collect("窗口 " + start + " - " + end + ": <" + key + ", " + sum + ">");
    }
}