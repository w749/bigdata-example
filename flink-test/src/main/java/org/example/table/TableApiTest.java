package org.example.table;

import org.apache.flink.table.api.Table;
import org.example.util.SetUp;
import org.junit.Test;


import static org.apache.flink.table.api.Expressions.*;

/**
 * Table基础API
 *     创建表：所以表都是通过 Catalog 来进行注册创建的。表在环境中有一个唯一的 ID，由三部分组成：目录（catalog）名，数据库（database）名，以及表名。在默认情况下，目录名为 default_catalog，数据库名为default_database
 *     连接器表：最直观的创建表的方式，就是通过连接器（connector）连接到一个外部系统，然后定义出对应的表结构
 *     Explain执行计划：返回抽象语法树、逻辑优化计划和物理执行计划
 *     Append表和Changelog表
 *     建议使用SQL或者原生的Flink底层代码，Table夹在中间类似于Spark的DSL一样，应用面并不广
 *     这一部分还是看官网吧：https://nightlies.apache.org/flink/flink-docs-release-1.13/
 */
public class TableApiTest implements SetUp {
    @Test
    public void createTableTest() {
        // 执行DDL语句创建表
        tableEnv.executeSql(fileInput);
        tableEnv.executeSql(fileOutput);
        tableEnv.executeSql(consoleOutput);

        // 将创建在环境中的表转为Table
        Table inputTable = tableEnv.from("inputTable");

        // 将Table注册到环境中，就可以直接在sql语句中使用
//        tableEnv.createTemporaryView("inputTable", inputTable);

        // 使用sql语句
        Table concatTable = tableEnv.sqlQuery("select user_name, url from inputTable where user_name = 'Alice'");
        tableEnv.createTemporaryView("concatTable", concatTable);
        tableEnv.executeSql("INSERT INTO outputTable SELECT * FROM concatTable");

        // 直接调用Table的executeInsert方法写入目标表
        Table output = inputTable.where($("user_name").isEqual("Alice")).select($("user_name"), $("url"));
        output.executeInsert("outputTable");
        output.execute().print();  // 执行并打印到控制台
    }

    /**
     * Explain执行计划
     *     返回抽象语法树、逻辑优化计划和物理执行计划
     */
    @Test
    public void explainTest() {
        tableEnv.executeSql(fileInput);
        Table data = tableEnv.from("inputTable").where($("user_name").isEqual("Alice")).select($("user_name"), $("url"));
        System.out.println(data.explain());
        System.out.println(tableEnv.explainSql("select user_name, url from inputTable where user_name = 'Alice'"));
    }

    /**
     * Append表和Changelog表
     *     append是只有新增的数据，不会对之前的数据做更新删除，操作类型只有INSERT
     *     changelog则是会修改之前的数据，类似于状态更新，它的操作类型有INSERT和DELETE，更新操作则需要retract撤销原有数据再append需要更新的数据
     */
    @Test
    public void changelogTest() throws Exception {
        env.setParallelism(1);
        tableEnvStream.executeSql(fileInput);
        Table data1 = tableEnvStream.sqlQuery("select * from inputTable where user_name = 'Alice'");
        Table data2 = tableEnvStream.sqlQuery("select url, count(user_name) from inputTable group by url");
        tableEnvStream.toDataStream(data1).print("DataStream");  // 有更新操作的必须使用Changelog
        tableEnvStream.toChangelogStream(data2).print("ChangelogStream");
        env.execute();
    }
}
