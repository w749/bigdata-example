package org.example.datastream;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.example.util.PathUtils;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Sink输出
 *     实现SinkFunction接口（一般来说实现RichSinkFunction，它提供了完整的生命周期）
 *     StreamingFileSink输出到文件中，通过它可以输出到本地目录或HDFS目录
 *
 */
public class SinkTest implements SetUp {

    private static Properties kafkaProp = new Properties();

    /**
     * 输出到本地目录使用StreamingFileSink.forRowFormat，HDFS的parquet格式使用forBulkFormat
     *     需要传入两个参数分别是输出路径和编码格式，随后还可以对一些参数进行配置，最常用的就是withRollingPolicy对文件大小和时间进行配置
     */
    static {
    // 配置Kafka连接信息
        kafkaProp.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer .class.getName());
        kafkaProp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    }
    @Test
    public void fileSinkTest() throws Exception {
        // 写出到本地目录下
        StreamingFileSink<String> fileSink = StreamingFileSink.forRowFormat(new Path(PathUtils.getPath("data/output/file")),  // 输出路径
                        new SimpleStringEncoder<String>("UTF-8"))  // 编码格式
                // 设置按时间或文件大小生成新文件的规则
                .withRollingPolicy(DefaultRollingPolicy.builder()
                        .withMaxPartSize(1024 * 1024)  // 超过1M生成新文件
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))  // 超过15分钟生成新文件
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))  // 若5分钟没有新数据写入生成新文件
                        .build())
                .build();

        data.map(Event::toString).setParallelism(4).addSink(fileSink);
        env.execute();
    }

    /**
     * 从Kafka消费数据并生产数据到Kafka实现WordCount，注意两个topic不能相同
     */
    @Test
    public void kafkaSinkTest() throws Exception {
        env.addSource(new FlinkKafkaConsumer<String> ("test", new SimpleStringSchema(), kafkaProp))
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }).returns(new TypeHint<Tuple2<String, Long>>() {})
                .keyBy(data -> data.f0)
                .sum(1)
                .setParallelism(4)
                .map(tuple -> tuple.f0 + ", " + tuple.f1)
                .addSink(new FlinkKafkaProducer<String>("test1", new SimpleStringSchema(), kafkaProp));

        env.execute();
    }
}
