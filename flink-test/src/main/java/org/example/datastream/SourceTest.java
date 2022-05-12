package org.example.datastream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.source.MySource;
import org.example.util.PathUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;

public class SourceTest {
    private StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /**
     * 直接从文本文件或者Collection中获取数据
     */
    @Test
    public void sourceTest1() throws Exception {
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Alice", "./123", 1000L));
        events.add(new Event("Bob", "./456", 500L));
        // 从Collection中获取
        env.fromCollection(events).print("从Collection中获取");

        // 直接输入元素
        env.fromElements(
                new Event("Alice", "./123", 1000L),
                new Event("Bob", "./456", 500L)
        ).print("直接输入元素");

        // 从文件中获取
        env.readTextFile(PathUtils.getPath("data/input/Event")).print("从文件中获取");
        // 从端口获取数据
//        env.socketTextStream("localhost", 7777);
        env.execute();
    }

    /**
     * Flink作为Kafka的Consumer消费数据
     */
    @Test
    public void kafkaTest() throws Exception {
        // 配置Kafka连接信息
        Properties kafkaProp = new Properties();
        kafkaProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // 添加Kafka消费者
        env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), kafkaProp))
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(new TypeHint<Tuple2<String, Long>>() {})
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(data -> data.f0)
                .sum(1)
                .print();
        env.execute();
    }

    /**
     * 使用自定义的Source
     */
    @Test
    public void mySourceTest() throws Exception {
        env.addSource(new MySource()).setParallelism(2).print();
        env.execute();
    }
}
