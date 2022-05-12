package org.example.wc;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.util.PathUtils;
import org.junit.Test;

/**
 * Flink分别使用DataStream和DataSet API操作WordCount
 */
public class FlinkWordCount {
    // 创建环境
    private StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    private ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();


    /**
     * DataSet批处理执行WordCount
     */
    @Test
    public void batchWordCount() throws Exception {
        env.readTextFile(PathUtils.getPath("data/input/worjpsdCount"))  // 获取数据源
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {  // 对数据源按行进行处理，需要传入一个定义输入和输出的lambda表达式
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));  // 收集就是输出
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG))  // 最后需要指定输出类型,后续操作有类型会好操作
                .groupBy(0)  // 按元组的第一个位置group
                .sum(1)  // 将第二个位置的数值加起来
                .sortPartition(1, Order.DESCENDING)  // 按第二列降序
                .print();  // 打印输出
    }

    /**
     * DataSteam流处理执行WordCount（有界流式处理）
     * 打印输出的前面的数字是线程号，默认是电脑核数
     * 相同的key被分到同一个线程中方便统计处理
     */
    @Test
    public void streamWordCount() throws Exception {
        streamEnv.readTextFile(PathUtils.getPath("data/input/wordCount"))
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)  // 按元组的第一个位置group，这里和batch操作有些不一样
                .sum(1)
                .print();  // 打印输出，需要execute才能执行
        streamEnv.execute();
    }

    /**
     * 无界数据流WordCount
     * 使用socket发送数据
     */
    @Test
    public void streamNonLimit() throws Exception {
        streamEnv.socketTextStream("127.0.0.1", 1234)
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)  // 按元组的第一个位置group，这里和batch操作有些不一样
                .sum(1)
                .print();  // 打印输出，需要execute才能执行
        streamEnv.execute();

    }
}
