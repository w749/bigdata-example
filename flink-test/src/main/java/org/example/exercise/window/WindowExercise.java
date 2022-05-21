package org.example.exercise.window;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import org.example.util.Event;
import org.junit.Test;

import java.time.Duration;

/**
 * 窗口函数练习
 *     包含了乱序流Watermark、标记事件时间、事件时间开窗、窗口延迟关闭、侧输出流、增量聚合函数和全窗口函数等
 *     测试数据在flink-test/data/input/Event文件中
 */
public class WindowExercise {
    @Test
    public void run() throws Exception {
        // 数据源
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("localhost", 7777);
        OutputTag<Event> tag = new OutputTag<Event>("late") {};

        // 原始数据生成两秒的水位线并指定事件时间
        SingleOutputStreamOperator<Event> data = source.map(new MapWindowFunction())  // 将字符串数据包装成Event
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))  // 乱序流水位线设置两秒
                        .withTimestampAssigner(((element, recordTimestamp) -> element.getTime())))  // 标记事件时间
                .returns(new TypeHint<Event>() {});  // 防止范型擦除
        data.print("original");

        // 窗口计算并加上一分钟的延迟关闭时间
        SingleOutputStreamOperator<WindowResult> agg = data.keyBy(Event::getUrl)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))  // 按事件时间开窗10秒
                .allowedLateness(Time.minutes(1))  // 允许迟到数据进入窗口，窗口延迟关闭一分钟
                .sideOutputLateData(tag)  // 错过watermark和延迟关闭的时间就放到侧输出流
                .aggregate(new UVAggFunc(), new UVProcessFunc());  // 增量聚合函数和全窗口函数结合使用
        agg.print("window");

        // 侧数据流的数据
        agg.getSideOutput(tag).print("late");  // 单纯打印侧数据流，生产中可以对agg的结果追加计算并更新

        env.execute();
    }
}
