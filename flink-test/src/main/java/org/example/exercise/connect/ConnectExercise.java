package org.example.exercise.connect;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.time.Duration;

/**
 * Connect方法练习
 *     实现对比App支付日志和第三方平台支付日志中的支付记录，查验是否两边都接收到了同一个order的数据
 */
public class ConnectExercise {
    @Test
    public void connectTest() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // App支付日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appSource = env.fromElements(
                Tuple3.of("order-1", "app", 1000L), Tuple3.of("order-2", "app", 2000L), Tuple3.of("order-3", "app", 3000L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((element, recordTimestamp) -> element.f2)
                );
        // 第三方平台支付日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> partSource = env.fromElements(
                Tuple3.of("order-1", "part", 3000L), Tuple3.of("order-2", "part", 4000L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((element, recordTimestamp) -> element.f2)
                );

        // 合并流将同一个order的数据放在一起并使用状态和定时器判断两条流的数据是否都到了
        appSource.connect(partSource)
                .keyBy(app -> app.f0, part -> part.f0)
                .process(new CoProcessFunc())
                .print();
        env.execute();
    }
}
