package org.example.sql;

import org.apache.flink.table.api.TableConfig;
import org.example.util.SetUp;
import org.junit.Test;

import java.time.Duration;

/**
 * SQL常用操作
 *     创建Source和Sink表
 *     Watermark、事件时间、处理时间
 *     开窗计算、Over开窗
 *     状态TTL
 *     实现TopN
 *     Join操作
 *     Deduplication去重
 *     系统函数：https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/functions/systemfunctions/
 */
public class CommonTest implements SetUp {
    /**
     * 可以把连接外部数据源当作sql新建表的操作，source和sink都可以这样操作
     *     Flink SQL支持常见的FileSystem、ES、Kafka、JDBC、HBase、Hive等作为Source或者Sink
     *     除了字段之外必要的连接参数都在with关键字中
     *     外部数据源查看：https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/overview/
     *     建表语句查看：https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/sql/create/
     *
     */
    @Test
    public void createTableTest() {
        // 以kafka source为例说明
        // METADATA是所属连接的元数据，例如kafka的timestamp就是消费数据的时间戳，类似的还有topic、partition等等
        // `event_time` TIMESTAMP(3) METADATA FROM 'timestamp' 意思是将消费数据的时间作为event_time属性值
        // `offset` BIGINT METADATA VIRTUAL 则是将消费kafka的offset作为属性列，若元数据是只读的则必须指定VIRTUAL，这样INSERT时就会忽略
        System.out.println(kafkaInput);
    }

    /**
     * watermark
     *     事件时间、处理时间
     */
    @Test
    public void watermarkTest() {
        // WATERMARK FOR `ts` as `ts` - INTERNAL '5' second"
        // 上方就是定义一个Watermark，在已有字段的基础上新建一个5s的watermark，注意ts字段必须是TIMESTAMP或者TIMESTAMP_LTZ类型，这时就是以ts字段作为事件时间计算
        // 如果按处理时间则直接新定义一个字段将它作为处理时间即可：`ts` AS PROCTIME()
        System.out.println(kafkaInput);
    }

    /**
     * Window
     *     Flink1.12之前有一个老版本的开窗方法，实现操作较复杂，且有些功能实现不了
     *     Flink1.13开始主要使用新版本开窗方法，窗口表值函数，使用更为方便，且功能强大
     *     官网：https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/dev/table/sql/queries/window-tvf/
     */
    @Test
    public void windowTest() {
        // 老版本支持TUMBLE（滚动窗口）、HOP（滑动窗口）、SESSION（会话窗口）
        // 老版本窗户聚合方法，需要将开窗动作写在group by当中，聚合计算就和普通sql一样放在select之后
        String oldTumble = "SELECT " +
                "  user, " +
                "  TUMBLE_END(ts, INTERVAL '1' HOUR) as endT " +
                "  COUNT(url) as cnt " +
                "FROM eventTable" +
                "GROUP BY " +
                "  user, " +
                "  TUMBLE(ts, INTERVAL '1' HOUR)";

        // 新版本支持TUMBLE、HOP、CUMULATE（累积窗口），会话窗口暂时还不支持
        // 除了原始表中的所有列，新增了三个额外的描述窗口的列：window_start、window_end、window_time（窗口时间，window_end-1ms，指的是窗口内最大的时间戳）
        String newTumble = "TUMBLE(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOUR)";  // DESCRIPTOR内传入事件时间字段
        String newHop = "HOP(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '5' MINUTES, INTERVAL '1' HOURS));";  // 最后两个参数分别是，滑动步长，滑动间隔
        String cumulate = "CUMULATE(TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOURS, INTERVAL '1' DAYS))";  // 实现每天滚动计算一次，但是每一小时输出一天开始到现在的结果
        // 新版本使用方法，直接把开窗行为传递到TABLE函数中然后group by起始窗口时间，这是固定的写法
        String windowTumble = "SELECT " +
                "  user, " +
                "  window_end AS endT, " +
                "  COUNT(url) AS cnt " +
                "FROM TABLE( " +
                "  TUMBLE( TABLE EventTable, " +
                "    DESCRIPTOR(ts), " +
                "    INTERVAL '1' HOUR)) " +
                "GROUP BY user, window_start, window_end";
    }

    /**
     * 状态TTL
     *     防止状态过大造成资源浪费，所以在sql操作中也支持设置状态TTL
     */
    @Test
    public void stateTtlTest() {
        TableConfig tableConfig = tableEnv.getConfig();
        tableConfig.setIdleStateRetention(Duration.ofDays(1));  // 设置状态TTL为一天
    }

    /**
     * Over开窗
     *     上面所说的window是固定的时间间隔和步长，是和Flink底层开窗相对应的
     *     over开窗则是以当前行的记录为基准，往前包含指定的rows或者time interval来开窗，和认知中mysql的操作类似
     */
    @Test
    public void overTest() {
        // over的几个组成部分是PARTITION BY、ORDER BY和开窗范围（以时间划定范围、以行数划定范围）
        String overPattern = "agg_func(agg_col) OVER ( " +
                "    [PARTITION BY col1[, col2, ...]] " +
                "    ORDER BY time_col " +
                "    range_definition)";
        String rangeScope = "RANGE BETWEEN INTERVAL '30' MINUTE PRECEDING AND CURRENT ROW";  // 以时间划定范围
        String rowsScope = "ROWS BETWEEN 10 PRECEDING AND CURRENT ROW";  // 以行数划定范围

        // 两种使用方法
        String over01 = "SELECT order_id, order_time, amount, " +
                "  SUM(amount) OVER ( " +
                "    PARTITION BY product " +
                "    ORDER BY order_time " +
                "    RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW " +
                "  ) AS one_hour_prod_amount_sum " +
                "FROM Orders";  // 和mysql类似直接写在字段中
        String over02 = "SELECT order_id, order_time, amount, " +
                "  SUM(amount) OVER w AS sum_amount, " +
                "  AVG(amount) OVER w AS avg_amount " +
                "FROM Orders " +
                "WINDOW w AS ( " +
                "  PARTITION BY product " +
                "  ORDER BY order_time " +
                "  RANGE BETWEEN INTERVAL '1' HOUR PRECEDING AND CURRENT ROW)";  // 写在WINDOW中并指定名称，之后就可以重复使用，增加代码可读性
    }

    /**
     * 实现TopN
     */
    @Test
    public void topNTest() {
        // 常规使用over开窗实现累计topN
        String overTopN = "SELECT * " +
                "FROM ( " +
                "  SELECT *, " +
                "    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS row_num " +
                "  FROM ShopSales) " +
                "WHERE row_num <= 5";
        // 实现窗口内topN，只需把over内的表换成开窗后的结果表
        String windowTopN = "SELECT * " +
                "  FROM ( " +
                "    SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price DESC) as rownum " +
                "    FROM ( " +
                "      SELECT window_start, window_end, supplier_id, SUM(price) as price, COUNT(*) as cnt " +
                "      FROM TABLE( " +
                "        TUMBLE(TABLE Bid, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES)) " +
                "      GROUP BY window_start, window_end, supplier_id " +
                "    ) " +
                "  ) WHERE rownum <= 3;";
    }
    /**
     * Join操作
     *     1. INNER JOIN、LEFT JOIN、RIGHT JOIN、FULL OUTER JOIN
     *     2. 间隔联结，就是底层InternalJoin的sql实现，时间间隔的指定支持三种写法
     *         lTime = rTime
     *         lTime >= rTime AND lTime < rTime + INTERVAL '10' MINUTE
     *         lTime BETWEEN rTime - INTERVAL '10' SECOND AND rTime + INTERVAL '5' SECOND
     *     3. Temporal Join：看官网解释它就是BroadcastState广播流状态的实现，左边的数据流联结右边的版本流，从版本流中取最新的版本数据与数据流联结
     *       注意这个最新的定义是需要事件时间或者处理时间来确定的，可以查看org.example.exercise.state.OperatorStateTest#broadcastTest()方法
     *
     */
    @Test
    public void joinTest() {
        // 1. 内联结和普通sql含义相同，另外同样可以实现左外联结、右外连接和全外联结，目前只支持等值联结
        String innerJoin = "SELECT * " +
                "FROM Order " +
                "INNER JOIN Product " +
                "ON Order.product_id = Product.id";

        // 2. 间隔联结
        String intervalJoin = "SELECT * " +
                "FROM Order o, Shipment s " +
                "WHERE o.id = s.order_id " +
                "AND o.order_time BETWEEN s.ship_time - INTERVAL '4' HOUR AND s.ship_time";

        // 3. 时间联结，或者广播联结
        // 根据事件时间获取版本流（currency_rates）中的最新版本（conversion_rate），注意两条流必须都设置了事件时间和watermark
        String temEventJoin = "SELECT order_id, price, currency, conversion_rate, order_time " +
                "FROM orders " +
                "LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF orders.order_time " +
                "ON orders.currency = currency_rates.currency";
        // 使用处理时间更好理解一些，broadcastTest测试方法使用的就是处理时间，它始终返回最新的版本表内容与数据流联结，利用proctime关键字指定
        String temProcessJoin = "SELECT o.amount, o.currency, r.rate, o.amount * r.rate " +
                "FROM Orders AS o " +
                "  JOIN LatestRates FOR SYSTEM_TIME AS OF o.proctime AS r " +
                "  ON r.currency = o.currency";
    }

    /**
     * Deduplication去重
     *     利用over开窗后取row_number为1的记录即可实现全局去重
     */
    @Test
    public void dupTest() {
        String dup = "SELECT order_id, user, product, num " +
                "FROM ( " +
                "  SELECT *, " +
                "    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY proctime ASC) AS row_num " +
                "  FROM Orders) " +
                "WHERE row_num = 1";
    }
}
