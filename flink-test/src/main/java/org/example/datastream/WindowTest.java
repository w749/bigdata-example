package org.example.datastream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.aggregate.MyAgg;
import org.example.aggregate.MyAgg01;
import org.example.aggregate.MyProcess01;
import org.example.util.Event;
import org.example.util.SetUp;
import org.junit.Test;

/**
 * 对于无界数据流定义指定的窗口对里面的数据进行聚合是很常用的操作，一般不会对无界数据流从头开始做聚合
 * 窗口常常会配合Watermark一起使用，如果Watermark延迟一定时间进来不属于这个窗口的数据那么会重新创建一个新窗口不会把不属于这个窗口的数据算进来
 * 窗口分类
 *     按照驱动类型分为时间窗口（窗口的有固定的时间界限，时间到窗口关闭）和计数窗口（按进入窗口的数据量决定，没有固定的时间界限）
 *     按照分配规则
 *         滚动窗口（Tumbling Window）：窗口有固定的大小，并且窗口之间没有重叠，第一个窗口关闭就开始下一个窗口
 *         滑动窗口（Sliding Window）：和滚动窗口不一样的是窗口之间的滚动是有时间步长的，步长小于窗口时间就会重叠，两个时间相等就和滚动窗口相同
 *         会话窗口（Session Window）：与滚动窗口不同的是会话窗口没有固定的大小，但有一个超时时间，超过超时时间才会关闭窗口。注意Flink底层每来一个数据就开启一个新的窗口，比较如果小于超时时间就会对两个窗口进行merge
 *         全局窗口（Global Window）：全局窗口没有结束时间，默认也不会触发计算，Flink底层的计数窗口就是全局窗口。如果要对数据进行处理就要自定义触发器Trigger
 * Keyed Non-Keyed
 *     按键分区：数据经过keyBy操作后再开窗计算，多个key之间的计算相互不影响，会分到多个TaskManager所以计算压力没那么大
 *     非按键分区：所有数据都当作一个key执行，会把数据分到一个task上，相当于并行度为1，手动调整并行度也是无用的
 * WindowFunction
 *     开窗后返回的是WindowStream，开场后必须带有聚合函数才会转为DataStream
 *     增量聚合函数
 *         规约函数reduce：和keyBy后使用的ReduceFunction是同一个类
 *         聚合函数aggregate：reduce返回值的类型必须和输入类型相同，那么就有了AggregateFunction更灵活的处理规约
 *     全窗口函数
 *         相比于增量聚合函数它是等这一个窗口的数据全部到齐了才开始计算
 *         窗口函数apply：实现WindowFunction接口，它是老版本的实现接口，后面会被逐渐弃用
 *         处理窗口函数process：继承ProcessWindowFunction类，相比WIndowFunction可以获取的状态更多
 *     实际中可以将两个结合使用，增量窗口函数用来计算，窗口结束后结果输出给全窗口函数，由它包装窗口信息再输出
 */
public class WindowTest implements SetUp {
    /**
     * 几种常用的开窗形式，开窗后返回的是WindowStream，开场后必须带有聚合函数才会转为DataStream
     */
    @Test
    public void windowTypeTest() {
        // 滚动窗口，一秒滚动一次，用的是处理时间
        dataKeyBy.window(TumblingAlignedProcessingTimeWindows.of(Time.seconds(1)));
        // 滚动窗口，一秒滚动一次，用的是事件时间，第二个参数是窗口开始的偏移量，在时差计算时很有用
        dataKeyBy.window(TumblingEventTimeWindows.of(Time.seconds(1), Time.hours(-8)));
        // 滑动窗口，一秒滑动一次，步长为50毫秒
        dataKeyBy.window(SlidingAlignedProcessingTimeWindows.of(Time.seconds(1), Time.milliseconds(50)));
        // 滑动窗口，一秒滑动一次，步长为50毫秒，事件时间，第三个参数是窗口开始的偏移量，在时差计算时很有用
        dataKeyBy.window(SlidingEventTimeWindows.of(Time.seconds(1), Time.milliseconds(50), Time.hours(-8)));
        // 会话窗口，超过1秒没进数就开启一个新窗口，处理时间
        dataKeyBy.window(ProcessingTimeSessionWindows.withGap(Time.seconds(1)));
        // 会话窗口，超过1秒没进数就开启一个新窗口，事件时间
        dataKeyBy.window(EventTimeSessionWindows.withGap(Time.seconds(1)));
        // 会话窗口，根据元素内容提取会话时间，需要重写extract方法
        dataKeyBy.window(ProcessingTimeSessionWindows.withDynamicGap(element -> element.getUser().length() * 100L));
        // 滑动计数窗口，每十个数统计一次，每过两个数滑动一次，只有一个参数就是滚动计数窗口
        dataKeyBy.countWindow(10, 2);
    }

    /**
     * 增量聚合函数
     */
    @Test
    public void windowFuncTest() {
        // 规约函数，这里和keyBy后用的ReduceFunction是同一个
        dataKeyBy.countWindow(10).reduce(((value1, value2) -> value1.getTime() > value2.getTime() ? value1 : value2));
        // 聚合函数，自定义实现求属性平均值
        dataKeyBy.countWindow(10).aggregate(new MyAgg());
    }

    /**
     * 全窗口函数
     *     它不能定义定时器，需要定时器在trigger里定义
     */
    @Test
    public void windowFunc2Test() {
        // WindowFunction
        dataKeyBy.countWindow(10).apply(new WindowFunction<Event, Tuple2<String, Long>, String, GlobalWindow>() {
            Long tm = 0L;
            @Override
            public void apply(String key, GlobalWindow window, Iterable<Event> input, Collector<Tuple2<String, Long>> out) throws Exception {
                input.forEach(event -> tm += event.getTime());
                out.collect(Tuple2.of(key, tm));
            }
        });
        // ProcessWindowFunction
        dataKeyBy.window(TumblingAlignedProcessingTimeWindows.of(Time.seconds(1)))
                .process(new ProcessWindowFunction<Event, Tuple2<String, Long>, String, TimeWindow>() {
                    Long tm = 0L;
                    @Override
                    public void process(String key, ProcessWindowFunction<Event, Tuple2<String, Long>, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<Tuple2<String, Long>> out) throws Exception {
                        context.window().getStart();  // 窗口开始时间
                        context.window().getEnd();  // 窗口结束时间
                        elements.forEach(event -> tm += event.getTime());
                        out.collect(Tuple2.of(key, tm));
                    }
                });
    }

    /**
     * 增量聚合函数和全窗口函数结合使用
     */
    @Test
    public void windowFunc3Test() {
        dataKeyBy.window(TumblingAlignedProcessingTimeWindows.of(Time.seconds(1)))
                .aggregate(new MyAgg01(), new MyProcess01());
    }
}
