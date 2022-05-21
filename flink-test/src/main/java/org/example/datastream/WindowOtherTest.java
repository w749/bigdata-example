package org.example.datastream;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.OutputTag;
import org.example.aggregate.MyAgg;
import org.example.util.Event;
import org.example.util.SetUp;
import org.junit.Test;

/**
 * 触发器Trigger、移除器Evictor、允许延迟和侧输出流
 *     注意这部分操作都是在window和aggregate之间完成的，为满足不同的业务需求，提高数据完整性
 */
public class WindowOtherTest implements SetUp {
    /**
     * 自定义触发器Trigger
     * 主要是根据每个元素、事件时间和处理时间来控制什么时候调用窗口函数和关闭窗口
     *     TriggerResult就是控制调用窗口函数和关闭窗口的，在需要的时候返回不同的枚举值
     *         CONTINUE：什么都不做
     *         FIRE_AND_PURGE：提交计算并销毁窗口
     *         FIRE：提交计算
     *         PURGE：销毁窗口
     */
    @Test
    public void triggerTest() {
        dataKeyBy.window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .trigger(new MyTrigger());
    }

    /**
     * 自定义移除器Evictor
     *     目的是在满足一定条件的时候移除其他数据
     *     需要重写两个方法evictBefore和evictAfter，分别是在执行WindowFunction之前移除和之后移除，区别在于是否参与计算
     */
    @Test
    public void evictorTest() {
        dataKeyBy.window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .evictor(new MyEvictor());
    }

    /**
     * 窗口内允许延迟
     *     它和Watermark的作用类似，都是尽可能的将迟到数据纳入计算中去
     *     相比于Watermark设定全局的数据延迟allowedLateness仅作用于当前计算的每个窗口
     *     只有当时间进行到了Watermark+延迟时间，窗口才会被销毁，在这个时间到来之前数据仍然可以进入计算
     */
    @Test
    public void lateTest() {
        dataKeyBy.window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .allowedLateness(Time.seconds(1));
    }

    /**
     * 侧输出流
     *     对于那些超过Watermark和窗口内延迟的数据我们可以将它放在侧输出流（Side Output）保存这些迟到的数据
     *     需要调用sideOutputLateData传入一个OutputTag标记
     *     最后通过计算完成的DataStream通过getSideOutput可以获取到那些迟到的原始数据
     *     再对这些数据进行计算合并到之前的计算结果中就是最终准确的结果
     */
    @Test
    public void sideTest() {
        OutputTag<Event> tag = new OutputTag<Event>("side"){};  // 防止泛型擦除
        SingleOutputStreamOperator<Double> side = dataKeyBy.window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .sideOutputLateData(tag)
                .aggregate(new MyAgg());

        DataStream<Event> sideOutput = side.getSideOutput(tag);
    }

    /**
     * 自定义Trigger
     */
    static class MyTrigger extends Trigger<Event, TimeWindow> {
        // 窗口每进来一个元素都要调一下这个方法
        @Override
        public TriggerResult onElement(Event element, long timestamp, TimeWindow window, Trigger.TriggerContext
        ctx) throws Exception {
            return null;
        }

        // 当注册的处理时间定时器触发时调用这个方法
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
            return null;
        }

        // 当注册的事件时间定时器触发时调用这个方法
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, Trigger.TriggerContext ctx) throws Exception {
            return null;
        }

        // 当窗口关闭时调用这个方法，类似于生命周期的close
        @Override
        public void clear(TimeWindow window, Trigger.TriggerContext ctx) throws Exception {

        }
    }

    /**
     * 自定义Evictor
     */
    static class MyEvictor implements Evictor<Event, TimeWindow> {
        // 执行WindowFunction之前移除数据，不会参与计算
        @Override
        public void evictBefore(Iterable<TimestampedValue<Event>> elements, int size, TimeWindow window, Evictor.EvictorContext
        evictorContext) {

        }

        // 执行WindowFunction之后移除数据，参与计算后再移除
        @Override
        public void evictAfter(Iterable<TimestampedValue<Event>> elements, int size, TimeWindow window, Evictor.EvictorContext
        evictorContext) {

        }
    }
}
