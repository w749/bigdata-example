package org.example.datastream;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.example.util.Event;
import org.example.util.SetUp;
import org.junit.Test;

import java.sql.Timestamp;

/**
 * 基本处理函数ProcessFunction
 *   它和其他类似map和filter不同的地方在于它提供了一个定时服务TimerService，可以通过它访问流中的事件、时间戳、Watermark等，发送侧输出流，甚至可以注册定时事件，而且它继承了AbstractRichFunction，拥有富函数类的特性
 *   通过调用process方法即可传入对应的处理函数，Flink提供了八种处理函数
 *   ProcessFunction：最基本的处理函数，基于 DataStream直接调用.process()时作为参数传入
 *   KeyedProcessFunction：对流按键分区后的处理函数，基于KeyedStream调用.process()时作为参数传入。要想使用定时器，比如基于KeyedStream
 *   ProcessWindowFunction：开窗之后的处理函数，也是全窗口函数的代表。基于WindowedStream调用.process()时作为参数传入
 *   ProcessAllWindowFunction：同样是开窗之后的处理函数，基于AllWindowedStream调用.process()时作为参数传入
 *   CoProcessFunction：合并（connect）两条流之后的处理函数，基于ConnectedStreams调用.process()时作为参数传入
 *   ProcessJoinFunction：间隔连接（interval join）两条流之后的处理函数，基于IntervalJoined调用.process()时作为参数传入。
 *   BroadcastProcessFunction：广播连接流处理函数，基于BroadcastConnectedStream调用.process()时作为参数传入。这里的“广播连接流”BroadcastConnectedStream，
 *     是一个未keyBy的普通DataStream与一个广播流（BroadcastStream）做连接（conncet）之后的产物
 *   KeyedBroadcastProcessFunction：按键分区的广播连接流处理函数，同样是基于 BroadcastConnectedStream 调用.process()时作为参数传入。
 *     与BroadcastProcessFunction不同的是，这时的广播连接流，是一个KeyedStream与广播流（BroadcastStream）做连接之后的产物
 */
public class ProcessFunctionTest implements SetUp {
    /**
     * ProcessFunction实现
     *     继承ProcessFunction类定义输入输出泛型重写processElement方法即可
     *     一般不用它，不支持定时器且功能比较少
     */
    @Test
    public void processTest() throws Exception {
        data.process(new MyProcessFunction()).print();
        env.execute();
    }

    /**
     * KeyedProcessFunction实现
     *     相比ProcessFunction多了定时器的设定
     */
    @Test
    public void keyProcessTest() throws Exception {
        dataKeyBy.process(new MyKeyedProcessFunction()).print();
        env.execute();
    }


    /**
     * 实现ProcessFunction
     */
    static class MyProcessFunction extends ProcessFunction<Event, String> {

        @Override
        public void processElement(Event value, ProcessFunction<Event, String>.Context ctx, Collector<String> out) throws Exception {
            Timestamp timestamp = new Timestamp(ctx.timestamp());
            Timestamp watermark = new Timestamp(ctx.timerService().currentWatermark());
            Timestamp processTime = new Timestamp(ctx.timerService().currentProcessingTime());
//                ctx.timerService().registerEventTimeTimer(10000);  // ProcessFunction不能注册定时器，只能在KeyedProcessFunction注册
            out.collect(value.toString() + ", timestamp: " + timestamp + ", watermark：" + watermark + ", processTime：" + processTime);
        }
    }

    /**
     * 实现KeyedProcessFunction
     *     注意事件时间的定时器是根据Watermark触发的，而Watermark默认是200ms间隔才更新一次，所以它一直是滞后的，当前数据到了以后才会根据上一个数据的事件时间减一更新Watermark
     *         意思是如果设置10秒的定时器，进来一个1秒的数据和11秒的数据，这时不会触发1秒数据的定时器，因为此时的watermark是1秒减去1毫秒是999毫秒，只有大于11秒的数据进来才会按11秒的数据把Watermark更新为10999毫秒，此时就会触发1秒数据的定时器
     */
    static class MyKeyedProcessFunction extends KeyedProcessFunction<String, Event, String> {
        @Override
        public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
            long processTime = ctx.timerService().currentProcessingTime();
            long eventTime = ctx.timestamp();
            // 注册一个10秒的事件时间定时器,注意要指定触发的长整形时间戳，定时器可以是处理时间也可以是事件时间
//            ctx.timerService().registerProcessingTimeTimer(processTime + Time.seconds(10).toMilliseconds());
            ctx.timerService().registerEventTimeTimer(eventTime + Time.seconds(10).toMilliseconds());
            out.collect(ctx.getCurrentKey() + "数据到达，当前时间为：" + new Timestamp(processTime));
        }

        // 定时器触发时要进行的操作
        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "定时器触发，触发时间：" + new Timestamp(timestamp));
        }
    }
}
