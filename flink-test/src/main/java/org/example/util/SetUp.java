package org.example.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.Duration;
import java.util.Calendar;
import java.util.Random;

/**
 * 环境准备
 */
public interface SetUp {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    SingleOutputStreamOperator<Event> data = env.addSource(new EventSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2)));
    KeyedStream<Event, String> dataKeyBy = data.keyBy(Event::getUrl);

    // 随机生成数据
    class EventSource implements SourceFunction<Event> {
        String[] users = {"Alice", "Bob", "FriendMan", "Booby", "Divide", "Tom", "File"};
        String[] urls = {"abc", "dfe", "xyz", "www"};
        Random random = new Random();
        boolean running = true;
        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            while (running) {
                String user = users[random.nextInt(users.length)];
                String url = urls[random.nextInt(urls.length)];
                long tm = Calendar.getInstance().getTimeInMillis() - 1000 * 60;
                ctx.collectWithTimestamp(new Event(user, url, tm), tm);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

