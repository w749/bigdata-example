package org.example.datastream;

import org.example.util.SetUp;
import org.junit.Test;

/**
 * Flink Transform算子
 *     map、filter、flatMap
 *     keyBy（类似于groupBy，想要聚合只能先将key相同的归到一起，它是使用hashCode来判断是否相同的，自定义POJO类需要重写hashCode方法）
 *         max：只更新需要求最大值的那个属性，其他属性采用第一条数据的属性值
 *         maxBy：默认更新连同最大属性的之外所有属性的对象，跟第一条数据就没关系了，当第二个参数为false时跟max结果相同
 *         min、minBy：和max、maxBy一样
 *         reduce：规约聚合，两个参数分别是前一个和后一个元素，对它们进行比较、聚合等操作后再返回一个元素
 */
public class TransformTest implements SetUp {
    /**
     * 简单聚合min、minBy、max、maxBy
     */
    @Test
    public void aggTest() throws Exception {
//        dataKeyBy.max("time").print("Max");
//        dataKeyBy.maxBy("time").print("MaxBy");

        dataKeyBy.min("time").print("Min");
        dataKeyBy.minBy("time").print("MinBy");
        env.execute();
    }

    /**
     * 规约操作reduce
     */
    @Test
    public void aggTest2() throws Exception {
        dataKeyBy.reduce(
                (value1, value2) -> {return value1.getTime() > value2.getTime() ? value1 : value2;})
                .print("reduce");
        env.execute();
    }
}
