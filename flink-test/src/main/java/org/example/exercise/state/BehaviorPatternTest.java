package org.example.exercise.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 假设数据流中每个用户有两条连续的行为，广播流定义连续的行为具体的规则
 * 判断连续的两条用户行为数据是否满足广播流中的规则，满足则输出
 */
public class BehaviorPatternTest extends KeyedBroadcastProcessFunction<String, BehaviorPatternTest.Action, BehaviorPatternTest.Pattern, String> {
    private ValueState<String> userActionState;
    MapStateDescriptor<String, Pattern> patternStateDesc = new MapStateDescriptor<>("patterns", Types.STRING, Types.POJO(Pattern.class));

    @Override
    public void open(Configuration parameters) throws Exception {
        userActionState = getRuntimeContext().getState(new ValueStateDescriptor<>("user-action", String.class));
    }

    @Override
    public void processElement(Action value, KeyedBroadcastProcessFunction<String, Action, Pattern, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
        // 取出processBroadcastElement处理的广播状态，这个状态是只读的
        Pattern pattern = ctx.getBroadcastState(patternStateDesc).get("");
        String userAction = userActionState.value();
        if (userAction != null && pattern != null) {
            if (userAction.equals(pattern.getAction01()) && value.getAction().equals(pattern.getAction02())) {
                out.collect("User: " + ctx.getCurrentKey() + " -> " + pattern);
            }
        }
        userActionState.update(value.getAction());
    }

    @Override
    public void processBroadcastElement(Pattern value, KeyedBroadcastProcessFunction<String, Action, Pattern, String>.Context ctx, Collector<String> out) throws Exception {
        // 将最新的广播流数据更新到状态中
        BroadcastState<String, Pattern> patternState = ctx.getBroadcastState(patternStateDesc);
        patternState.put("", value);
    }



    /**
     * 用户行为
     */
    static class Action {
        private String name;
        private String action;

        public Action() {
        }

        public Action(String name, String action) {
            this.name = name;
            this.action = action;
        }

        public String getName() {
            return name;
        }

        public String getAction() {
            return action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "name='" + name + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    /**
     * 用户行为规则
     */
    public static class Pattern {
        private String action01;
        private String action02;

        public Pattern() {
        }

        public Pattern(String action01, String action02) {
            this.action01 = action01;
            this.action02 = action02;
        }

        public void setAction01(String action01) {
            this.action01 = action01;
        }

        public void setAction02(String action02) {
            this.action02 = action02;
        }

        public String getAction01() {
            return action01;
        }

        public String getAction02() {
            return action02;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action01='" + action01 + '\'' +
                    ", action02='" + action02 + '\'' +
                    '}';
        }
    }
}
