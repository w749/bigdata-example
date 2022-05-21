package org.example.exercise.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.util.Event;
import org.example.util.SetUp;
import org.junit.Test;

/**
 * ValueState练习
 *     实现统计每个url出现的次数，每隔十秒（根据事件时间）输出统计结果
 */
public class ValueStateTest implements SetUp {
    @Test
    public void valueTest() throws Exception {
        env.setParallelism(1);
        dataKeyBy.process(new ValueKeyedFunc(10)).print();
        env.execute();
    }
    static class ValueKeyedFunc extends KeyedProcessFunction<String, Event, Event> {
        private final long second;
        private ValueState<Long> countState;
        private ValueState<Boolean> timerState;

        public ValueKeyedFunc(long second) {
            this.second = second;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            // 需要两个状态分别是统计count次数和新建定时器
            countState = getRuntimeContext().getState(new ValueStateDescriptor<>("state-count", Long.class));
            timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("state-timer", Boolean.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, Event>.Context ctx, Collector<Event> out) throws Exception {
            out.collect(value);
            Long count = countState.value();
            countState.update(count == null ? 1 : countState.value() + 1);
            // 仅第一次在这设置定时器
            if (timerState.value() == null) {
                // 这个数据源Watermark设置的是两秒，所以设置10秒的定时器实际上12秒才会触发
                ctx.timerService().registerEventTimeTimer(value.getTime() + second * 1000);
                timerState.update(true);
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, Event>.OnTimerContext ctx, Collector<Event> out) throws Exception {
            System.out.println(ctx.getCurrentKey() + " 的出现次数为 " + countState.value());
            // 触发定时器后就把它清空，让它从当前的事件时间重新开始注册
            timerState.clear();
            ctx.timerService().registerEventTimeTimer(timestamp + second * 1000);
            timerState.update(true);
        }
    }
}
