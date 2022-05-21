package org.example.datastream;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.example.util.Event;
import org.example.util.SetUp;
import org.example.watermark.MyWaterMark;
import org.junit.Test;

import java.time.Duration;

/**
 * Watermark水位线
 *     处理时间：指的是数据到达处理算子时当前的系统时间。
 *     事件时间：指的是数据产生的时间，跟Flink本身没有关系。因为分布式系统无法保证数据处理的顺序和产生的顺序一致，所以就有了Watermark来解决这个问题
 *     在时间语义中不依赖系统时间，而是基于事件时间戳定义一个时钟用来表示当前时间的进展，于是每个子任务都有一个逻辑时钟，它是靠数据时间驱动的。
 *         为了避免上下游各子任务之间的时钟不一致，所以上游也会将时钟以数据的形式传递下去，下游所有子任务就会更新自己的时钟。这种用来标记事件时间进展的标记就被叫做Watermark
 *     有序流中的水位线：如果按每一条数据标记一次Watermark那么当数据量特别大时就会产生很多时间戳相同的数据，为此Flink提供周期性的插入Watermark，这个周期往往很短。注意这个周期是相对系统时间的
 *     乱序流中的水位线：有序流只是理想状态，实际某些数据会因为网络延迟打乱顺序，这个乱序的程度是需要解决的主要问题，比如晚到了1秒或者2秒，那么这个Watermark就应该有一个延迟机制，延迟一定的时间使得迟到的数据被算进来，不然它们就会被算到下一个窗口或者被筛选掉
 *     总结下来Watermark用来标记当前的事件时间，它提供了一个策略延迟一定的时间使得迟到的数据可以进来，但是这个周期性不会因为这个延迟机制而改变（属于下一个窗口的数据虽然被包在Watermark内但不会进入计算）
 *     假设设置Watermark延迟策略是1秒，那么如果窗口是0-10秒，那么事件时间为10秒的数据进来后watermark把它标记为9秒，当前窗口就不会关闭，只有11秒的数据进来才会关闭当前窗口
 *     Watermark默认是200ms间隔才更新一次，所以它一直是滞后的，当前数据到了以后才会根据上一个数据的事件时间减一更新Watermark
 *         假如进来两条数据的事件时间分别是1秒和11秒，那么第一条数据的Watermark应该取默认值为Long整形的最小值-1，第二条数据的Watermark为第一条数据的事件时间-1为999毫秒
 */
public class WatermarkTest implements SetUp {

    /**
     * 生成水位线
     *     一般在数据刚进Flink就生成水位线，它会跟着数据传递给下游算子，Flink提供了常用的Watermark策略
     *     WatermarkStrategy.forMonotonousTimestamps生成单调递增的Watermark
     *     WatermarkStrategy.forBoundedOutOfOrderness生成乱序数据的Watermark，要指定乱序程度，也就是延迟时间
     */
    @Test
    public void assignTest() {
        env.getConfig().setAutoWatermarkInterval(100);  // 设置Watermark发送时间间隔，Flink默认200ms
        data.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(100))
                                // 上方只是生成一个Watermark策略，还需要指定Watermark将数据中的哪个字段作为事件时间
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    // 主要重写extractTimestamp方法，告诉Watermark事件时间是哪个，返回毫秒数，这里可以用lambda
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.getTime();
                                    }
                                }));
    }

    /**
     * 自定义Watermark
     */
    @Test
    public void myWatermarkTest() {
        data.assignTimestampsAndWatermarks(
                new MyWaterMark().withTimestampAssigner(((element, recordTimestamp) -> element.getTime()))
        );
    }
}
