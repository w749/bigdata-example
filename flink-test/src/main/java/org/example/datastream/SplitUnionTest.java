package org.example.datastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.example.util.Event;
import org.example.util.SetUp;
import org.junit.Test;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * 合流分流
 *     Flink1.13版本之前使用split方法分流，新版本已经弃用，改为在ProcessFunction中使用侧输出流实现分流
 *     合流
 *         union：多条流的数据直接合并，数据类型必须相同，Watermark使用流中最小的Watermark，确保所有的数据都会被算进来，如果一条流的数据不再更新，那么union后的Watermark也不再更新
 *         connect：数据类型可以不统一，合并后返回ConnectedStreams，此时两条流的数据类型并没有统一，需要后续再使用map process处理后才会统一得到DataStream
 *         join：双流join，相比connect更加便捷且更符合写sql时join的习惯，且写法比较简单
 *         internalJoin：join只可以进行开窗后计算，internalJoin提供了一个时间窗口，对data1的每条数据限定一个前后时间范围，这条数据会与在这个时间范围内的data2所有数据join（key相同）
 *         coGroup：相比join最后的apply方法内只可以获取两条流到key相同的单个数据，coGroup是将同一个窗口内两条流的数据都放在iterable内哪怕另外一条流没有数据
 */
public class SplitUnionTest implements SetUp {

    /**
     * 使用ProcessFunction中的output方法就可以将数据发送到侧输出流，数据类型可以和原数据类型不同，随后再取出来做不同的操作
     */
    @Test
    public void splitTest() throws Exception {
        env.setParallelism(1);
        OutputTag<String> xyz = new OutputTag<String>("xyz") {};
        OutputTag<String> abc = new OutputTag<String>("abc") {};
        SingleOutputStreamOperator<Event> process = dataKeyBy.process(new KeyedProcessFunction<String, Event, Event>() {
            @Override
            public void processElement(Event value, KeyedProcessFunction<String, Event, Event>.Context ctx, Collector<Event> out) throws Exception {
                if (value.getUrl().equals("xyz")) {
                    ctx.output(xyz, xyz.getId() + "侧输出流数据：" + value);
                } else if (value.getUrl().equals("abc")) {
                    ctx.output(abc, abc.getId() + "侧输出流数据：" + value);
                }
            }
        });

        // 获取对应的侧输出流处理数据
        process.getSideOutput(xyz).print();
        process.getSideOutput(abc).print();
        env.execute();
    }

    /**
     * Union合流，多条流的数据类型必须相同
     */
    @Test
    public void unionTest() {
        data.union(data, data, data);
    }

    /**
     * Connect合流，用得最多，需要合并的数据类型可以不同
     *     因为是对两条流数据做处理，所以map需要传入的是CoMapFunction，process需要传入的是CoProcessFunction等等
     */
    @Test
    public void connectTest() throws Exception {
        env.setParallelism(1);
        DataStreamSource<Integer> stream1 = env.fromElements(1, 3, 5, 7, 9);
        DataStreamSource<String> stream2 = env.fromElements("a", "b", "c", "d", "e");

        stream1.connect(stream2)
                .map(new CoMapFunction<Integer, String, String>() {
                    @Override
                    public String map1(Integer value) throws Exception {
                        return "Stream1: " + value;
                    }

                    @Override
                    public String map2(String value) throws Exception {
                        return "Stream2: " + value;
                    }
                }).print();
        env.execute();
    }


    SingleOutputStreamOperator<Tuple2<String, Integer>> data1 = env.fromElements(
                    Tuple2.of("a", 3000), Tuple2.of("a", 5000), Tuple2.of("b", 1000), Tuple2.of("c", 3000), Tuple2.of("d", 4500)
            )
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner(((element, recordTimestamp) -> element.f1)));
    SingleOutputStreamOperator<Tuple2<String, Integer>> data2 = env.fromElements(
                    Tuple2.of("a", 2000), Tuple2.of("b", 500), Tuple2.of("a", 6000), Tuple2.of("c", 10000)
            )
            .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                    .withTimestampAssigner(((element, recordTimestamp) -> element.f1)));

    /**
     * 双流连接窗口Join
     *     它的固定流程就是stream1.join(stream2).where.equalTo.window.apply，点进源码去看就是一条唯一的流程
     */
    @Test
    public void joinTest()throws Exception {
        env.setParallelism(1);

        data1.join(data2)
                // 指定data1需要join的值
                .where(data -> data.f0)
                // 指定data2需要join的值
                .equalTo(data -> data.f0)
                // 开窗操作，所用的就是DataStream的window，返回同一个窗口内两个流相同join值的笛卡尔积
                .window(TumblingEventTimeWindows.of(Time.seconds(100)))
                // apply里面就是对两个相同join值的数据进行处理并输出，里面也可以传FlatJoinFunction
                .apply(((first, second) -> (first + " -> " + second)), Types.STRING)
                .print();
        env.execute();
    }

    /**
     * 双流连接间隔连接InternalJoin
     *     注意只有两条流都匹配到数据时才会进入process方法，所以有些时候要判断两条流的匹配情况还是得用connect
     *     调用顺序也是比较固定的，注意between中的lower要输入负数，因为底层是加上这个值来决定左边的范围
     */
    @Test
    public void internalJoinTest() throws Exception {
        env.setParallelism(1);

        data1.keyBy(data -> data.f0)
                // 这里传入两外一条流的keyBy后结果，把它跟data1的keyBy后结果key相同的进入后续计算
                .intervalJoin(data2.keyBy(data -> data.f0))
                // 利用事件时间做间隔
                .inEventTime()
                // 指定左边的时间间隔和右边的时间间隔，默认是左闭右闭
                .between(Time.seconds(-1), Time.seconds(2))
                // 不包括左边的点，大于而不是大于等于，改成左开右闭
                .lowerBoundExclusive()
                // process处理函数
                .process((new ProcessJoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple2<String, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        Timestamp leftTm = new Timestamp(ctx.getLeftTimestamp());
                        Timestamp rightTm = new Timestamp(ctx.getRightTimestamp());
                        out.collect(left + " -> " + right + ", leftTm: " + leftTm + ", rightTm: " + rightTm);
                    }
                })).print();
        env.execute();
    }

    /**
     * 双流CoGroup
     *     相比join最后的apply方法内只可以获取两条流到key相同的单个数据，coGroup是将同一个窗口内两条流的数据都放在iterable内哪怕另外一条流没有数据
     *     这样CoGroup的可操作性就会更强，它不光可以做内连接，外连接全连接都可以做
     */
    @Test
    public void coGroupTest() throws Exception {
        env.setParallelism(1);

        data1.coGroup(data2)
                .where(data -> data.f0)
                .equalTo(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(100)))
                .apply(new CoGroupFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Integer>> first, Iterable<Tuple2<String, Integer>> second, Collector<String> out) throws Exception {
                        out.collect(first + " -> " + second);
                    }
                }).print();

        env.execute();
    }
 }
