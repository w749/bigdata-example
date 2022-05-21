package org.example.exercise.state;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.util.Event;
import org.example.util.SetUp;
import org.junit.Test;

import java.sql.Timestamp;

/**
 * 利用MapState实现滚动窗口计数
 *     设定好窗口的开始时间和结束时间并设定状态，在同一个窗口内更新状态，然后设置一个定时器，在定时器内输出并销毁状态
 */
public class MapStateTest implements SetUp {
    @Test
    public void mapTest() throws Exception {
        env.setParallelism(1);
        dataKeyBy.process(new MapKeyedFunc(10L)).print();
        env.execute();
    }

    static class MapKeyedFunc extends KeyedProcessFunction<String, Event, String> {
        private final Long windowSize;
        private MapState<Long, Long> windowState;

        public MapKeyedFunc(Long windowSize) {
            this.windowSize = windowSize * 1000L;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            windowState = getRuntimeContext().getMapState(new MapStateDescriptor<>("state-map", Long.class, Long.class));
        }

        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            long windowStart = value.getTime() / windowSize * windowSize;
            long windowEnd = windowStart + windowSize;
            ctx.timerService().registerEventTimeTimer(windowEnd - 1);
            if (!windowState.contains(windowStart)) {
                windowState.put(windowStart, 1L);
            } else {
                windowState.put(windowStart, windowState.get(windowStart) + 1L);
            }
            out.collect(value.toString());
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            // 注意这里的开始和结束时间不要和processElement里的混用，一定要根据定时器传过来的timestamp，processElement可能同时出现两个窗口，这样开始和结束时间就会有变化
            long end = timestamp + 1;
            long start = end - windowSize;
            long count = windowState.get(start);
            out.collect("Window: " + ctx.getCurrentKey() + " count is " + count + " and start " + new Timestamp(start) + ", end " + new Timestamp(end));
            // 当前窗口数据输出完后就注销，这里没考虑延迟关闭和侧输出流，其实可以加上
            windowState.remove(start);
        }
    }
}
