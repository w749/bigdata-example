package org.example.exercise.state;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.example.util.SetUp;
import org.junit.Test;

/**
 * Operator State
 *     ListState
 *     BroadcastState
 */
public class OperatorStateTest implements SetUp {
    /**
     * ListState CheckpointedFunction使用
     *     一般常用在输出Sink中，用来做缓存输出，并且可以保存快照
     *     注意它的状态是针对每个并行子任务的，所以多个并行度要看每个并行任务是否满足阈值
     */
    @Test
    public void listTest() throws Exception {
        env.setParallelism(2);
        data.addSink(new BufferSinkTest(10));
        env.execute();
    }

    /**
     * BroadcastState
     *     一个数据流与一个广播流connect，根据广播流里的规则判断数据流里的数据是否符合规则
     */
    @Test
    public void broadcastTest() throws Exception {
        env.setParallelism(1);
        // 数据流
        DataStreamSource<BehaviorPatternTest.Action> actionStream = env.fromElements(
                new BehaviorPatternTest.Action("Alice", "login"),
                new BehaviorPatternTest.Action("Alice", "pay"),
                new BehaviorPatternTest.Action("Bob", "login"),
                new BehaviorPatternTest.Action("Bob", "order"),
                new BehaviorPatternTest.Action("Jone", "login"),
                new BehaviorPatternTest.Action("Jone", "reader")
        );

        // 广播流
        MapStateDescriptor<String, BehaviorPatternTest.Pattern> patternStateDesc =
                new MapStateDescriptor<>("patterns", Types.STRING, Types.POJO(BehaviorPatternTest.Pattern.class));
        BroadcastStream<BehaviorPatternTest.Pattern> patternStream = env.fromElements(
                new BehaviorPatternTest.Pattern("login", "pay"),
                new BehaviorPatternTest.Pattern("login", "order")
        ).broadcast(patternStateDesc);

        // 连接两条流并比对规则
        actionStream.keyBy(BehaviorPatternTest.Action::getName)
                .connect(patternStream)
                .process(new BehaviorPatternTest())
                .print();

        env.execute();
    }
}
