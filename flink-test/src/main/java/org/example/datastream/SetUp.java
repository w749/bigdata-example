package org.example.datastream;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * 环境准备
 */
public interface SetUp {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<Event> data = env.fromElements(
            new Event("Alice", "./cdf", 444L),
            new Event("Alice", "./abc", 333L),
            new Event("Alice", "./123", 1000L),
            new Event("Bob", "./456", 500L),
            new Event("Bob", "./678", 666L),
            new Event("Friend", "./ccc", 678L)
    );
    KeyedStream<Event, String> dataKeyBy = data.keyBy(Event::getUser);
}

