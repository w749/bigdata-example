package org.example.exercise.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.example.util.Event;

import java.util.ArrayList;

/**
 * 自定义Sink实现每十条数据输出一次，实际生产中可以作为批量缓存输出
 *     分别定义两个列表，bufferedListElements用来存储数据输出数据，到阈值就输出一次，checkpointState用来保存checkpoint
 *     将数据list直接交给列表算子状态，由它来保存快照，故障恢复
 */
public class BufferSinkTest implements CheckpointedFunction, SinkFunction<Event> {
    private final int threshold;
    private ArrayList<Event> bufferedListElements = new ArrayList<>();
    private ListState<Event> checkpointState;

    public BufferSinkTest(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public void invoke(Event value, Context context) throws Exception {
        // SinkFunction实现方法，每个数据调用一次
        bufferedListElements.add(value);
        System.out.println(value);
        if (bufferedListElements.size() > threshold) {
            // 达到阈值输出数据并清空
            System.out.println("\n输出开始");
            bufferedListElements.forEach(System.out::println);
            System.out.println("输出完毕\n");
            bufferedListElements.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 将数据list直接交给列表算子状态，因为bufferedListElements达到阈值就输出，所以需要快照的只有当前数据列表的数据，复制前需要先把状态之前的内容清空
        checkpointState.clear();
        checkpointState.addAll(bufferedListElements);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 初始化状态
        checkpointState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("", Event.class));
        if (context.isRestored()) {
            checkpointState.get().forEach(event -> bufferedListElements.add(event));
        }
    }
}
