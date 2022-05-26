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
 */
public class SQLTest implements SetUp {
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
}
