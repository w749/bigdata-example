package org.example.table;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.example.util.SetUp;
import org.junit.Test;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Table API环境
 */
public class EnvironmentTest implements SetUp {
    /**
     * 使用StreamExecutionEnvironment获取TableEnv和数据并输出
     */
    @Test
    public void envTest() throws Exception {
        env.setParallelism(1);
        // 从StreamExecutionEnvironment获取TableEnv和数据
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(data);

        // 使用sql的方式处理数据
        Table query1 = tableEnv.sqlQuery("select user, url from " + table);

        // 使用Table自带的API
        Table query2 = table.select($("user"), $("time"))
                .addColumns(concat("1", "qwe"))
                .where($("user").isEqual("Alice"));

        // 需要输出数据需要转回DataStream
        tableEnv.toDataStream(query1).print("SQL");
        tableEnv.toDataStream(query2).print("Table");

        env.execute();
    }

    /**
     * 直接从TableEnvironment创建TableEnv
     */
    public void tableEnvTest() {
        // 直接传入EnvironmentSettings创建TableEnv
        TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build());
    }
}
