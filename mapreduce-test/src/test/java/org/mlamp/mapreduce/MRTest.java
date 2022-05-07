package org.mlamp.mapreduce;

import org.mlamp.mapreduce.combiner.WCCombinerDriver;
import org.mlamp.mapreduce.comparable.PhoneComparableDriver;
import org.mlamp.mapreduce.compress.PhoneCompress;
import org.mlamp.mapreduce.mapjoin.OrderMapJoinDriver;
import org.mlamp.mapreduce.outputformat.LogDriver;
import org.mlamp.mapreduce.partitioner.PhonePartitioner;
import org.mlamp.mapreduce.phone.PhoneDriver;
import org.mlamp.mapreduce.reducejoin.OrderDriver;
import org.mlamp.mapreduce.wc.WordCountTest;

import org.junit.Test;

public class MRTest {
    private final String BASE_LOCATION = "/Users/mlamp/workspace/my/test-utils/mapreduce-test/data/";
    /**
     * WordCount
     */
    @Test
    public void worCountTest() {
        String[] args = {BASE_LOCATION + "wordcount", BASE_LOCATION + "output/1"};
        try {
            WordCountTest.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 使用自定义类作为Mapper类的value，需要实现Writable接口
     */
    @Test
    public void phoneTest() {
        String[] args = {BASE_LOCATION + "phoneFlow", BASE_LOCATION + "output/2"};
        try {
            PhoneDriver.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 自定义Partitioner，按key对手机号进行分区
     */
    @Test
    public void partitionerTest() {
        String[] args = {BASE_LOCATION + "phoneFlow", BASE_LOCATION + "output/3"};
        try {
            PhonePartitioner.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 使用自定义类作为Mapper的key，必须实现Comparable接口，因为key默认会进行排序
     * 将phoneTest的输出结果按总流量进行排序
     */
    @Test
    public void comparableTest() {
        String[] args = {BASE_LOCATION + "output/2", BASE_LOCATION + "output/4"};
        try {
            PhoneComparableDriver.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 自定义Combiner
     */
    @Test
    public void combinerTest() {
        String[] args = {BASE_LOCATION + "wordcount", BASE_LOCATION + "output/5"};
        try {
            WCCombinerDriver.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 自定义OutputFormat，实现一行里包含A的输出到A.log，其他的输出到B.log
     */
    @Test
    public void outputFormatTest() {
        String[] args = {BASE_LOCATION + "outputFormat", BASE_LOCATION + "output/6"};
        try {
            LogDriver.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * ReduceJoin
     */
    @Test
    public void reduceJoinTest() {
        String[] args = {BASE_LOCATION + "joinOrder", BASE_LOCATION + "joinProduct", BASE_LOCATION + "output/7"};
        try {
            OrderDriver.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * MapJoin
     */
    @Test
    public void mapJoinTest() {
        String[] args = {BASE_LOCATION + "joinProduct", BASE_LOCATION + "joinOrder", BASE_LOCATION + "output/8"};
        try {
            OrderMapJoinDriver.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 设置数据压缩
     */
    @Test
    public void compressTest() {
        String[] args = {BASE_LOCATION + "phoneFlow", BASE_LOCATION + "output/9"};
        try {
            PhoneCompress.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
