package org.example.datastream;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.example.util.PathUtils;
import org.junit.Test;

/**
 * Flink支持所有Java和Scala的类型，底层都做了包装，具体都实现了TypeInformation
 *     对于Java的基础类型在org.apache.flink.api.common.typeinfo包下
 *     对于复合数据类型在org.apache.flink.api.java.typeutils包下
 *     对于自定义类似Java Bean的类需要满足类是公共的和独立的（没有内部静态类）、类包含一个公共的无参构造方法、所有字段都是public且非final的，或者提供一个公共的getter和setter方法
 *     Flink有一个类型系统可以自动的获取函数的返回类型，从而获取对应的序列化器和反序列化器
 *     但在有些时候类型提示不够精确（lambda表达式），所以就需要类型提示（type hints），在算子后加上.returns(new TypeHint<Tuple2<String, Long>>() {})
 */
public class TypeTest {
    private StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    @Test
    public void typeTest() throws Exception {
        env.readTextFile(PathUtils.getPath("data/input/wordCount"))
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(new TypeHint<Tuple2<String, Long>>() {})  // 类型提示
                .keyBy(data -> data.f0)
                .sum(1)
                .print();
        env.execute();
    }

}
