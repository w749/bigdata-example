package org.example.datastream;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.example.util.Event;
import org.example.util.SetUp;
import org.junit.Test;

import java.util.HashMap;

/**
 * 状态State
 *     在流处理中，数据是连续不断到来和处理的。每个任务进行计算处理时，可以基于当前数据直接转换得到输出结果；也可以依赖一些其他数据。这些由一个任务维护，并且用来计算输出结果的所有数据，就叫作这个任务的状态。
 *     有状态算子：除当前数据之外，还需要一些其他数据来得到计算结果。其他数据就是所谓的状态（state），最常见的就是之前到达的数据，或者由之前数据计算出的某个结果。
 *     无状态算子：只需要观察每个独立事件，根据当前输入的数据直接转换输出结果，类似map、filter
 *     要获取算子的状态就需要获取到它的runtime（getRuntimeContext），它属于AbstractRichFunction抽象类
 *     Flink 的状态有两种：托管状态（Managed State）和原始状态（Raw State）。托管状态就是由 Flink 统一管理的，状态的存储访问、故障恢复和重组等一系列问题都由 Flink 实现，我们只要调接口就可以；
 *       而原始状态则是自定义的，相当于就是开辟了一块内存，需要我们自己管理，实现状态的序列化和故障恢复。
 *     托管状态又可以分为：算子状态（Operator State）和按键分区状态（Keyed State）
 *     算子状态（Operator State）：状态作用范围限定为当前的算子任务实例，也就是只对当前并行子任务实例有效。这就意味着对于一个并行子任务，占据了一个“分区”，它所处理的所有数据都会访问到相同的状态，状态对于同一任务而言是共享的
 *     按键分区状态（Keyed State）：状态是根据输入流中定义的键（key）来维护和访问的，所以只能定义在按键分区流（KeyedStream）中，也就keyBy之后才可以使用。任务按照键（key）来访问和维护的状态。以key为作用范围进行隔离。
 *     因为一个并行子任务可能会处理多个 key 的数据，所以 Flink 需要对 Keyed State 进行一些特殊优化。在底层，Keyed State 类似于一个分布式的映射（map）数据结构，所有的状态会根据 key 保存成键值对（key-value）的形式
 *     在并行度改变时，状态也需要进行重组。不同key对应的 Keyed State可以进一步组成所谓的键组（key groups），每一组都对应着一个并行子任务。键组是Flink重新分配Keyed State的单元，键组的数量就等于定义的最大并行度。
 *       当算子并行度发生改变时，Keyed State就会按照当前的并行度重新平均分配
 */
public class StateTest implements SetUp {
    /**
     * 使用不同的State统计每条url来的数据
     */
    @Test
    public void stateTest() throws Exception {
        env.setParallelism(1);
        dataKeyBy.flatMap(new MyFlatMap()).print();
        env.execute();
    }

    /**
     * flatMap函数需要，要获取状态所以继承RichFlatMapFunction
     */
    static class MyFlatMap extends RichFlatMapFunction<Event, String> {
        private ValueState<Integer> valueState;
        private ListState<String> listState;
        private MapState<String, Integer> mapState;
        private AggregatingState<Event, Integer> aggState;
        private final HashMap<String, Integer> map = new HashMap<>();

        @Override
        public void open(Configuration parameters) throws Exception {
             valueState = getRuntimeContext().getState(new ValueStateDescriptor<>("state-value", Integer.class));
             listState = getRuntimeContext().getListState(new ListStateDescriptor<>("state-list", String.class));
             mapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("state-map", String.class, Integer.class));
             aggState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<>("state-agg", new AggStateFunc(), Integer.class));
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {
            out.collect("---------------\n" + value);
            // ValueState
            Integer count = valueState.value();
            valueState.update(count == null ? 1 : count + 1);
            out.collect( "ValueState: " + value.getUrl() + " 已生成 " + valueState.value() +  " 个数据");

            // ListState
            listState.add(value.getUser());
            out.collect("ListState: " + value.getUrl() + " 的用户有 " + listState.get());

            // MapState
            mapState.put(value.getUrl(), valueState.value());
            mapState.keys().forEach(key -> {
                try {
                    map.put(key, mapState.get(key));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
            out.collect("MapState: " + value.getUrl() + " 数据量为 " + map);

            // AggregatingState
            aggState.add(value);
            out.collect("AggregatingState: " + value.getUrl() + " 数据量为 " + aggState.get());
            out.collect("---------------\n");
        }

        /**
         * AggregatingStateDescriptor需要，实现简单的计数
         */
        static class AggStateFunc implements AggregateFunction<Event, Integer, Integer> {

            @Override
            public Integer createAccumulator() {
                return 0;
            }

            @Override
            public Integer add(Event value, Integer accumulator) {
                return accumulator + 1;
            }

            @Override
            public Integer getResult(Integer accumulator) {
                return accumulator;
            }

            @Override
            public Integer merge(Integer a, Integer b) {
                return a + b;
            }
        }
    }
}
