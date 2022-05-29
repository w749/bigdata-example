package org.example.sql;

import static org.apache.flink.table.api.Expressions.*;

import org.apache.flink.table.api.Table;
import org.example.util.SetUp;
import org.junit.Test;


/**
 * UDF
 *     官网：https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/functions/udfs/
 *     Scalar Function：一对一函数，输入一个输出一个
 *     Table Function：输入可以是0、1或者多行，不同的是可以返回多行数据，类似于Hive的Lateral View
 *     Aggregate Function：聚合函数，输入多行返回一行
 *     Table Aggregate Function：表聚合函数，输入多行返回多行
 */
public class FunctionTest implements SetUp {
    /**
     * Scalar Function
     *     需要自定义一个类来继承抽象类ScalarFunction，并实现叫作eval()的求值方法。标量函数的行为就取决于求值方法的定义，它必须是公有的（public），
     *     而且名字必须是eval。求值方法eval可以重载多次，任何数据类型都可作为求值方法的参数和返回值类型。注意ScalarFunction抽象类中并没有定义eval()方法
     */
    @Test
    public void scalarTest(){
        // 注册临时udf
        tableEnv.createTemporarySystemFunction("CONCAT_NAME", SQLFunc.ConcatNameFunc.class);

        // 注册后可以直接在sql中使用
        tableEnv.sqlQuery("SELECT CONCAT_NAME(user_name), url FROM inputTable");
        // 可以使用Table API，在call中先传入类class或函数名后接需要传入的参数
        tableEnv.from("inputTable").select(call(SQLFunc.ConcatNameFunc.class, $("user_name")), $("url"));
        tableEnv.from("inputTable").select(call("CONCAT_NAME", $("user_name")), $("url"));
    }

    /**
     * Table Function
     *     固定写法就是LATERAL TABLE(LATERAL_FUNC(name))
     *     或者使用LEFT JOIN LATERAL TABLE(LATERAL_FUNC(name)) ON TRUE
     *     后可接AS T(word, length)重命名返回的字段
     */
    @Test
    public void tableTest() {
        Table table = tableEnvStream.fromValues("Alice,Bob,Li", "Alice,Same,Join").as("name");
        tableEnvStream.createTemporaryView("tmpData", table);
        tableEnvStream.createTemporarySystemFunction("LATERAL_FUNC", SQLFunc.LateralFunc.class);
        tableEnvStream.sqlQuery("SELECT name, word, length " +
                "FROM tmpData, " +
                "LATERAL TABLE(LATERAL_FUNC(name)) AS T(word, length)")
                        .execute().print();
    }

    /**
     * Aggregate Function
     *     accumulate()与之前的求值方法 eval()类似，也是底层架构要求的，必须为 public，方法名必须为 accumulate，且无法直接 override、只能手动实现
     *     retract()从累加器实例中收回输入的值，必须为无界表上的有界 OVER 聚合实现此方法
     *     merge()将一组累加器实例合并为一个累加器实例。必须为无界会话窗口分组聚合和有界分组聚合实现此方法
     */
    @Test
    public void aggTest() {
        tableEnv.executeSql(aggInput);
        tableEnv.createTemporarySystemFunction("AVG_FUNC", SQLFunc.AvgFunc.class);
        tableEnv.sqlQuery("SELECT name, AVG_FUNC(score, weight) AS res FROM tmp GROUP BY name").execute().print();
    }

    /**
     * Table Aggregate Function
     *     与Aggregate Function不同的是Agg只输出一个数值或者一行数据，而TableAgg提供了Collector，使用它的collect方法可以向下游发送多条聚合数据
     *     accumulate、retract和merge参考Agg函数，另外TableAgg额外提供了emitValue用来发送数据，提供了emitUpdateWithRetract用来将retract和emitValue方法的功能合并在一起
     *     RankTableFunc02中实现了emitUpdateWithRetract方法
     */
    @Test
    public void tableAggTest() {
        tableEnv.executeSql(aggInput);
        tableEnv.from("tmp")
                .groupBy($("name"))
                .flatAggregate(call(SQLFunc.RankTableFunc.class, $("score")).as("value", "rank"))
                .select($("name"), $("value"), $("rank"))
                .execute()
                .print();
    }
}
