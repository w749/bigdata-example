package org.example.datastream;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.example.util.SetUp;
import org.junit.Test;

/**
 * 物理分区
 *     可以理解为重分区，类似与keyBy，相较于keyBy按照指定key的值进行分区重分区则提供了几种常见的分区方式按照一定的规则对数据进行分区
 *     shuffle：对上一个算子的数据打乱后随机选择分区传入到下一个算子
 *     rebalance：对上一个算子的数据进行分区轮训传入到下一个算子，Flink默认就是rebalance
 *     rescale：跟rebalance差不多，但是不同的是它是分组进行轮训，相较于rebalance可能会跨TaskManager rescale的效率会更高
 *     broadcast：将上游所有数据发送给下游每一个分区中
 *     global：强行将上游所有分区的数据归到一个分区中，此时下游设置分区也没用
 */
public class TransformPartitionTest implements SetUp {
    /**
     * 测试shuffle
     * 首先使得整体的并行度为1，那么data只有一个分区，随后shuffle随机分区到print四个分区中
     */
    @Test
    public void shuffleTest() throws Exception {
        env.setParallelism(1);
        data.shuffle().print().setParallelism(4);
        env.execute();
    }

    /**
     * 测试rebalance
     * 注意输出是按数据顺序进行分区轮训到print，只是print打印时没按顺序
     */
    @Test
    public void rebalanceTest() throws Exception {
        env.setParallelism(1);
        data.rebalance().print().setParallelism(2);
        env.execute();
    }

    /**
     * 测试rescale
     * 例如上游有两个分区，下游有六个分区，那么上游两个分区就会各自对应三个分区轮训
     * 假设有两个TaskManager，上游算子有两个分区分别在两个TaskManager上，下游有六个，那么rescale就会在各自的TaskManager内完成分区轮训
     */
    @Test
    public void rescaleTest() throws Exception {
        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 0; i <= 8; i++) {
                    // 使得奇数发到1号分区，偶数发到0号分区
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        ctx.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }) // fromElements不能重新设置分区
                .setParallelism(2)
                .rescale()
                .print().setParallelism(4);
        env.execute();
    }

    /**
     * 测试broadcast
     * 不管上游有几个分区，都将所有数据平均发送给下游的所有分区
     */
    @Test
    public void broadcastTest() throws Exception {
        env.setParallelism(1);
        data.broadcast().print().setParallelism(2);
        env.execute();
    }

    /**
     * 测试global
     * 不管上下游有几个分区，将所有的数据都归到一个分区中
     */
    @Test
    public void globalTest() throws Exception {
        env.setParallelism(2);
        data.global().print().setParallelism(4);
        env.execute();
    }

    /**
     * 自定义分区方法partitionCustom
     * 推荐使用的是传入两个参数，第二个参数是对需要判断的输入值进行转换然后输出，注意这里的转换操作是内部操作，不影响最终的数据输出；
     *     第一个参数是根据第二个参数的输出判断最终需要输出到哪个分区
     */
    @Test
    public void myPartitionTest() throws Exception {
        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 0; i <= 8; i++) {
                    if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                        ctx.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).partitionCustom(new Partitioner<Integer>() {
            // 分区方法，这里key的类型和KeySelector输出类型是相同的
            @Override
            public int partition(Integer key, int numPartitions) {
                return key % 2 + 1;
            }
        }, new KeySelector<Integer, Integer>() {
            // 对输入数据进行处理后再经过分区方法，注意这里的操作不影响最终输出的数据，仅影响partition的输入key
            @Override
            public Integer getKey(Integer value) throws Exception {
                return value * 2;
            }
        })
//                .partitionCustom(((key, num) -> key % 2 + 1), (value -> value + 1))  // 或者可以采用lambda表达式,但是注意这样要使用returns指定类型
                .print().setParallelism(4);

        env.execute();
    }
}
