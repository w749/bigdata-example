package org.example.exercise.connect;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 通过ValueState和TimeTimer判断同一key下的两条流数据是否都进来
 */
public class CoProcessFunc extends KeyedCoProcessFunction<String, Tuple3<String, String, Long>, Tuple3<String, String, Long>, String> {
    private ValueState<Tuple3<String, String, Long>> appState;
    private ValueState<Tuple3<String, String, Long>> partState;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 定义状态变量用来保存每一条来的数据
        appState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("app-state", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
        );
        partState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("part-state", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
        );
    }

    @Override
    public void processElement1(Tuple3<String, String, Long> value, KeyedCoProcessFunction<String, Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
        // 现在appSource这条流查看对应key的partSource有没有进来，进来的话就返回成功并清除状态
        if (partState.value() != null) {
            out.collect("Success, APP data is: " + value + ", and Part data is: " + partState.value());
            partState.clear();
        } else {  // 如果还没来就先更新appState的状态并设定时器等五秒
            appState.update(value);
            ctx.timerService().registerEventTimeTimer(value.f2 + 5000L);
        }
    }

    @Override
    public void processElement2(Tuple3<String, String, Long> value, KeyedCoProcessFunction<String, Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
        // 和appSource的思路一样
        if (appState.value() != null) {
            out.collect("Success, APP data is: " + appState.value() + ", and Part data is: " + value);
            appState.clear();
        } else {  // 因为给的数据都是appSource时间在前，那么partSource定时器可以少设定时间或者直接让它现在触发。如果是乱序数据Watermark是上一条数据时间-1，所以设置当前时间的定时器appSource数据肯定会来
            partState.update(value);
            ctx.timerService().registerEventTimeTimer(value.f2);
        }
    }

    @Override
    public void onTimer(long timestamp, KeyedCoProcessFunction<String, Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        // 定时器触发，判断状态是否为空，如果某个状态不为空说明另一条流数据没来（processElement触发说明来数了，就会将另一条流状态清除，那么至少会有一个状态为null）
        if (appState.value() != null) {
            out.collect("Failed, APP data is: " + appState.value() + ", Part data is null");
        }
        if (partState.value() != null) {
            out.collect("Failed, Part data is: " + partState.value() + ", APP data is null");
        }
        // 完事清理状态，始终确保初始都是null
        appState.clear();
        partState.clear();
    }
}
