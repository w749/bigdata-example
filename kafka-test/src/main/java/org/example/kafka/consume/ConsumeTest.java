package org.example.kafka.consume;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.Test;

import java.time.Duration;
import java.util.*;

/**
 * 消费者Consume
 */
public class ConsumeTest {
    private static Properties properties;
    private static ArrayList<String> topics = new ArrayList<>();
    static {
        properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "bj-mlue-5:9092,bj-mlue-6:9092,bj-mlue-7:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test2");  // gourp.id，必须指定

        topics.add("test123");
    }

    /**
     * 消费者消费数据，一个消费者消费一个主题的数据
     */
    @Test
    public void consumeTest() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topics);
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }

    /**
     * 拉取指定主题下的指定分区数据
     */
    @Test
    public void consumePartitionTest() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        ArrayList<TopicPartition> topicPartitions = new ArrayList<>();
        topicPartitions.add(new TopicPartition(topics.get(0), 0));  // 可以拉取多个分区的数据

        consumer.assign(topicPartitions);  // 注册指定的分区
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }

    /**
     * 指定Offset消费
     */
    @Test
    public void consumeSeekOffset() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topics);
        Set<TopicPartition> topicPartitions = new HashSet<>();

        // 需要先获取topic分区信息，有可能获取分区的时候subscribe还没进行完成，所以需要判断后持续获取
        while (topicPartitions.size() == 0) {
            consumer.poll(Duration.ofSeconds(1));
            topicPartitions = consumer.assignment();
            System.out.println("获取TopicPartitions");
        }
        // 从每个分区中获取指定offset之后的数据
        topicPartitions.forEach(topicPartition -> consumer.seek(topicPartition, 700));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }

    /**
     * 从指定时间开始消费
     */
    @Test
    public void consumeSeekTime() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topics);
        Set<TopicPartition> topicPartitions = new HashSet<>();
        HashMap<TopicPartition, Long> topicPartitionLong = new HashMap<>();

        // 需要先获取topic分区信息，有可能获取分区的时候subscribe还没进行完成，所以需要判断后持续获取
        while (topicPartitions.size() == 0) {
            consumer.poll(Duration.ofSeconds(1));
            topicPartitions = consumer.assignment();
            System.out.println("获取TopicPartitions");
        }

        // 将topic分区和指定的时间传入map，然后要将时间转换为当时所对应的offset再从指定的offset来获取数据
        topicPartitions.forEach(topicPartition -> topicPartitionLong.put(topicPartition, System.currentTimeMillis() - 24*3600*1000));
        Map<TopicPartition, OffsetAndTimestamp> topicPartitionOffsetAndTimestamp = consumer.offsetsForTimes(topicPartitionLong);

        // 最后获取指定分区的转换后的offset
//        System.out.println(topicPartitionLong);
//        System.out.println(topicPartitionOffsetAndTimestamp);
        topicPartitions.forEach(topicPartition -> {
            OffsetAndTimestamp offsetAndTimestamp = topicPartitionOffsetAndTimestamp.get(topicPartition);
            consumer.seek(topicPartition, offsetAndTimestamp.offset());
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }
}
