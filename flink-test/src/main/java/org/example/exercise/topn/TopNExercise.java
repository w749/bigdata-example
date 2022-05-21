package org.example.exercise.topn;

import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.example.util.Event;
import org.example.util.SetUp;
import org.junit.Test;

/**
 * 实现统计同一个窗口内TopN PV的url
 *     最简单的方法是使用ProcessAllWindowFunction不使用keyBy分开统计url，所有url计算
 *     然后就是先按url统计完之后再将统计结果收集起来排序处理
 */
public class TopNExercise implements SetUp {
    /**
     * 使用windowAll配合ProcessAllWindowFunction完成TopN的统计
     *     效率太低，无法并行
     */
    @Test
    public void windowAllTest() throws Exception {
        env.setParallelism(1);
        // 将所有数据都放到一个窗口内完成
        dataKeyBy.map(Event::getUrl)
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlAggFuncAll(), new UrlProcessFuncAll(2))
                .print();
        env.execute();
    }

    /**
     * 利用正常方法按每个url先统计并包装成UrlViewCount，统计完之后再将他们收集起来排序输出
     *     流程是先将流按窗口结束时间keyBy，就会把每个窗口所有的url对应的UrlViewCount都收集起来
     *     收集要使用ListState，随后创建一个定时器，时间不用特别长，因为代码是顺序处理的，所以判定定时器时间是在收集完数据之后
     *     最后在onTimer里处理收集到的数据
     */
    @Test
    public void topNTest() throws Exception {
        env.setParallelism(1);
        // 按照url分组统计每个窗口的访问量
        dataKeyBy.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlAggFunc(), new UrlProcessFunc())
                .keyBy(UrlViewCount::getEnd)  // 按每个UrlViewCount的结束时间为key，收集同一个窗口内数据
                .process(new KeyedProcessFunc(2))  // 对同一个窗口统计出来的访问量，收集并排序
                .print();
        env.execute();
    }


}
