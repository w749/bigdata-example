package org.example.util;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Calendar;
import java.util.Random;

/**
 * 环境准备
 */
public interface SetUp {
    // Stream环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    SingleOutputStreamOperator<Event> data = env.addSource(new EventSource())
            .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2)));
    KeyedStream<Event, String> dataKeyBy = data.keyBy(Event::getUrl);


    // Table环境
    TableEnvironment tableEnv = TableEnvironment.create(
            EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build());
    StreamTableEnvironment tableEnvStream = StreamTableEnvironment.create(env);
    // 创建表SQL语句
    String fileInput = "CREATE TABLE inputTable(" +
            "  user_name STRING, " +
            "  url STRING, " +
            "  ts BIGINT ) " +
            "WITH (" +
            "  'connector' = 'filesystem', " +
            "  'path' = '/Users/mlamp/workspace/my/test-utils/flink-test/data/input/Event', " +
            "  'format' = 'csv' " +
            ")";
    String fileOutput = "CREATE TABLE outputTable(" +
            "  user_name STRING, " +
            "  url STRING ) " +
            "WITH (" +
            "  'connector' = 'filesystem', " +
            "  'path' = '/Users/mlamp/workspace/my/test-utils/flink-test/data/output/table', " +
            "  'format' = 'csv' " +
            ")";
    String consoleOutput = "CREATE TABLE outputTable2(" +
            "  user_name STRING, " +
            "  url STRING ) " +
            "WITH (" +
            "  'connector' = 'print' " +
            ")";
    String kafkaInput = "CREATE TABLE KafkaTable (" +
            "  `user_name` STRING, " +
            "  `url` STRING, " +
            "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'" +
            "  WATERMARK FOR `ts` as `ts` - INTERNAL '5' second" +
            ") WITH ( " +
            "  'connector' = 'kafka', " +
            "  'topic' = 'user_behavior', " +
            "  'properties.bootstrap.servers' = 'localhost:9092', " +
            "  'properties.group.id' = 'testGroup', " +
            "  'scan.startup.mode' = 'earliest-offset', " +
            "  'format' = 'csv' " +
            ")";
    String aggInput = "CREATE TABLE tmp ( " +
            "  name StRING, " +
            "  score INT, " +
            "  weight INT ) " +
            "WITH ( " +
            "  'connector' = 'filesystem', " +
            "  'path' = '/Users/mlamp/workspace/my/test-utils/flink-test/data/input/sqlAgg', " +
            "  'format' = 'csv' " +
            ")";

    // 随机生成数据Source
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

