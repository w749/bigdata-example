package org.example.exercise.topn;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * 使用状态加定时器将上游同一个窗口内每个url统计好的数据都收集起来做计算
 *     每来一个数据都加入ListState，随后设定一个1毫秒的定时器
 *     在定时器中对ListState的数据进行排序输出
 */
public class KeyedProcessFunc extends KeyedProcessFunction<Long, UrlViewCount, String> {
    private final Integer top;
    private ListState<UrlViewCount> listState;  // 定义一个状态

    public KeyedProcessFunc(Integer top) {
        this.top = top;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 从运行时环境中将对应的状态取出来赋值给listState
        listState = getRuntimeContext().getListState(
                new ListStateDescriptor<UrlViewCount>("view-count", Types.POJO(UrlViewCount.class))
        );
    }

    @Override
    public void processElement(UrlViewCount value, KeyedProcessFunction<Long, UrlViewCount, String>.Context ctx, Collector<String> out) throws Exception {
        // 将上游来的每一个UrlViewCount加入ListState
        listState.add(value);
        // 处理时间加1毫秒设定时器，这里设置事件时间定时器不管用
        ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1);
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
        // 将收集好的数据转为ArrayList并排序
        ArrayList<UrlViewCount> viewCountList = new ArrayList<>();
        listState.get().forEach(viewCountList::add);
        viewCountList.sort((t1, t2) -> (int) (t2.getPv() - t1.getPv()));

        // 输出
        StringBuilder str = new StringBuilder();
        str.append("--------------------").append("\n");
        str.append("窗口结束时间：").append(new Timestamp(ctx.getCurrentKey())).append("\n");
        for (int i = 0; i < top; i ++) {
            String info = "No. " + (i + 1) +  " URL: " + viewCountList.get(i).getUrl() + ", PV: " + viewCountList.get(i).getPv();
            str.append(info).append("\n");
        }
        str.append("--------------------").append("\n");
        out.collect(str.toString());
    }
}
