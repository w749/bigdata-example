package org.example.kafka.partition;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区器
 *     实现Partitioner接口中的partition方法
 *     可以针对key也可以针对value
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String string = value.toString();
        if (string.toLowerCase().contains("one")) {
            return 0;
        } else if (string.toLowerCase().contains("two")) {
            return 1;
        } else {
            return 2;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
