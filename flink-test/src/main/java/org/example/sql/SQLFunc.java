package org.example.sql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;


public class SQLFunc {
    /**
     * Scalar Function
     */
    public static class ConcatNameFunc extends ScalarFunction {
        // DataTypeHint(inputGroup = InputGroup.ANY)对输入参数的类型做了标注，表示eval的参数可以是任意类型。
        public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
            return "Name: " + o.toString();
        }
    }

    /**
     * Table Function
     *     将输入分割后输出为Row，需要使用FunctionHint注解定义输出类型
     *     需要使用collect将输出发送给下游，数据需要一条一条输出
     */
    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class LateralFunc extends TableFunction<Row> {
        public void eval(String str) {
            for (String s : str.split(",")) {
                collect(Row.of(s, s.length()));
            }
        }
    }

    /**
     * Aggregate Function
     *     从学生的分数表 ScoreTable 中计算每个学生的加权平均分。从输入的每行数据中提取两个值作为参数：要计算的分数值 score，以及它的权重weight
     */
    public static class AvgFunc extends AggregateFunction<Integer, AvgAcc> {
        // 返回最后的结果调用
        @Override
        public Integer getValue(AvgAcc accumulator) {
            if (accumulator.cnt == 0) {
                return null;
            } else {
                return accumulator.sum / accumulator.cnt;
            }
        }

        // 定义累加器
        @Override
        public AvgAcc createAccumulator() {
            return new AvgAcc();
        }

        // 累加计算方法，每来一行数据都会调用。第一个参数是累加器，后面的参数都是需要传入的参数
        public void accumulate(AvgAcc acc, Integer score, Integer weight) {
            acc.sum += score * weight;
            acc.cnt += weight;
        }

        // 从累加器实例中收回输入的值，必须为无界表上的有界 OVER 聚合实现此方法。和accumulate一样，第一个参数是累加器，后面的参数都是需要传入的参数
        public void retract(AvgAcc acc, Integer score, Integer weight) {
            acc.sum -= score * weight;
            acc.cnt -= weight;
        }

        // 将一组累加器实例合并为一个累加器实例。必须为无界会话窗口分组聚合和有界分组聚合实现此方法。第一个参数为当前累加器，第二个参数为其他累加器类型的迭代器
        public void merge(AvgAcc acc, Iterable<AvgAcc> it) {
            it.forEach(otherAcc -> {
                acc.cnt += otherAcc.cnt;
                acc.sum += otherAcc.sum;
            });
        }
    }

    /**
     * Table Aggregate Function
     *     实现对输入的数据与上一个数据比较并rank输出
     */
    public static class RankTableFunc extends TableAggregateFunction<Tuple2<Integer, Integer>, RankAcc> {

        @Override
        public RankAcc createAccumulator() {
            RankAcc rankAcc = new RankAcc();
            rankAcc.first = Integer.MIN_VALUE;
            rankAcc.second = Integer.MIN_VALUE;
            return rankAcc;
        }

        // 每来一条数据计算一次。第一个参数是累加器，后面的参数都是需要传入的参数
        public void accumulate(RankAcc acc, Integer value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        // 合并当前累加器和其他累加器，参考AvgFunc的merge
        public void merge(RankAcc acc, Iterable<RankAcc> it) {
            it.forEach(otherAcc -> {
                accumulate(acc, otherAcc.first);
                accumulate(acc, otherAcc.second);
            });
        }

        // 发送数据到下游。两个参数分别是累加器和用于输出数据的收集器，使用out.collect发送数据
        public void emitValue(RankAcc acc, Collector<Tuple2<Integer, Integer>> out) {
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.first, 1));
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(Tuple2.of(acc.second, 2));
            }
        }
    }

    /**
     * Table Aggregate Function
     *     和RankTableFunc实现内容相同，使用emitUpdateWithRetract合并emitValue和retract功能
     */
    public static class RankTableFunc02 extends TableAggregateFunction<Tuple2<Integer, Integer>, RankAcc02> {

        @Override
        public RankAcc02 createAccumulator() {
            RankAcc02 acc = new RankAcc02();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            acc.firstOld = Integer.MIN_VALUE;
            acc.secondOld = Integer.MIN_VALUE;
            return acc;
        }

        public void accumulate(RankAcc02 acc, Integer value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        // 将发送数据和撤销数据放在一起，一个方法实现更新功能。第一个参数是累加器，第二个参数是发送数据的收集器RetractableCollector
        public void emitUpdateWithRetract(RankAcc02 acc, RetractableCollector<Tuple2<Integer, Integer>> out) {
            if (!acc.first.equals(acc.firstOld)) {
                if (acc.firstOld != Integer.MIN_VALUE) {
                    out.retract(Tuple2.of(acc.firstOld, 1));
                }
                out.collect(Tuple2.of(acc.first, 1));
                acc.firstOld = acc.first;
            }
            if (!acc.second.equals(acc.secondOld)) {
                if (acc.secondOld != Integer.MIN_VALUE) {
                    out.retract(Tuple2.of(acc.secondOld, 1));
                }
                out.collect(Tuple2.of(acc.second, 1));
                acc.secondOld = acc.second;
            }
        }
    }


    public static class AvgAcc {
        public int sum = 0;
        public int cnt = 0;
    }

    public static class RankAcc {
        public Integer first;
        public Integer second;
    }

    public static class RankAcc02 {
        public Integer first;
        public Integer second;
        public Integer firstOld;
        public Integer secondOld;
    }
}
