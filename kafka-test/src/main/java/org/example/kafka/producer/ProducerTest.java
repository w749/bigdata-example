package org.example.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.kafka.partition.MyPartitioner;
import org.junit.Test;

import java.util.Properties;

/**
 * Kafka生产者Producer
 *     分区器（partitioner.class）
 *         判断数据发送到哪个分区，默认是粘性分区器（尽量都发送到一个分区，直到达到batch.size大小再换其他的）
 *     批次大小（batch.size）、等待时间（linger.ms）
 *         数据会先放在缓冲区，默认32M，两个条件满足任意一个发送一批次到topic，批次大小默认16k，等待时间默认0ms
 *     提高吞吐量
 *         修改合适的批次大小和等待时间
 *         压缩数据（compression.type）
 *         修改缓冲区大小（buffer.memory）
 *     ISR
 *         Leader维护了一个动态的in-sync replica set（ISR），保存了和Leader保持同步的Follower+Leader集合（leader:0,isr:0,1,2）
 *         如果Follower长时间未与Leader发送通信请求或同步数据，则该Follower会被踢出ISR，该时间阈值由replica.lag.time.mac.ms参数控制，默认为30s
 *         ISR最小应答副本数min.insync.replicas默认为1（发送数据后最小应答副本数量）
 *     数据可靠性
 *         ACK：0（生产者发送过来的数据，不需要等数据落盘后应答）、1（Leader收到数据后应答）、-1（Leader+ISR队列里面的所有节点都收到数据后应答）
 *         retries：重试次数，默认为int的最大值
 *         数据完全可靠
 *             ACK设为-1 + 分区副本数大于等于2 + ISR里应答的最小副本数大于等于2（保证至少发送一次）
 *             幂等性和事务（0.11版本后，保证不重复）
 *                 幂等性
 *                     具有<PID,Partition,SeqNumber>相同主键的消息提交时，Broker只会持久化一条，其中PID是Kafka每次重启都会分配一个新的，Partition是分区号，SeqNumber是单调递增的
 *                     不管Producer向Broker发送了多少条数据，Broker只会持久化一条，保证不重复（只能保证单分区单回话内不重复）
 *                     开启参数enable.idempotence默认为true
 *                 事务
 *                     开启事务必须开启幂等性，并且必须指定一个全局唯一的transaction.id
 *     数据有序（单分区内）
 *         Kafka1.x之前需要将max.in.flight.requests.per.connection（每个队列最多允许的未应答请求）设为1
 *         Kafka1.x之后
 *             未开启幂等性时max.in.flight.requests.per.connection需要设为1
 *             开启幂等性后max.in.flight.requests.per.connection需要设置小于等于5（Kafka服务器会缓存最近5个请求的元数据）
 *                 因为幂等性中有一个单调递增的SeqNumber，若连续发过来的两个请求不是单调递增则存在内存中不落盘，等到本应该在这个位置的请求发过来才会按顺序落盘
 *
 */
public class ProducerTest {
    private static Properties properties;
    private String topic = "test123";
    static {
        properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "bj-mlue-5:9092,bj-mlue-6:9092,bj-mlue-7:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());
    }

    /**
     * 基础测试
     */
    @Test
    public void producerTest() {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        producer.send(new ProducerRecord<>(topic, "", "Hello"));
        producer.close();
    }

    /**
     * 使用自定义分区器发送数据
     */
    @Test
    public void producerPartitionTest() {
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        String[] strings = {"oneBob", "Bobone", "twoAlice", "threeBlack"};

        for (String name: strings) {
            producer.send(new ProducerRecord<>(topic, "", name), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        System.out.println("topic：" + recordMetadata.topic() + "，分区：" + recordMetadata.partition());
                    }
                }
            });
        }
        producer.close();
    }

    /**
     * 提高吞吐量
     */
    @Test
    public void raiseBatchTest() {
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32);  // 批次大小，k
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 10);  // 等待时间，ms
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);  // 缓冲区大小，k
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");  // 压缩类型snappy、gzip
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        String[] strings = {"oneBob", "Bobone", "twoAlice", "threeBlack"};
        for (String name: strings) {
            producer.send(new ProducerRecord<>(topic, "", name));
        }
        producer.close();
    }

    /**
     * 开启事务
     */
    @Test
    public void transactionTest() {
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional_id_01");  // 指定transactional.id
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        String[] strings = {"oneBob", "Bobone", "twoAlice", "threeBlack"};
        producer.beginTransaction();  // 开启事务
        try {
            for (String name: strings) {
                producer.send(new ProducerRecord<>(topic, "", name));
            }
            producer.commitTransaction();  // 提交事务
        } catch (Exception e) {
            producer.abortTransaction();  // 回滚事务
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    /**
     * 发送多条数据测试
     */
    @Test
    public void producerLargeData() {
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<>(topic, "", "" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println(metadata.topic() + ", " + metadata.partition() + ", " + metadata.offset());
                }
            });
        }
        producer.close();
    }
}
