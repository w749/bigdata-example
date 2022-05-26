package org.example.datastream;

/**
 * Flink状态一致性
 *     这里所说的一致性就是结果的正确性，在Flink中分为三个等级
 *         最多一次（AT-MOST-ONCE）：不做任何一致性策略，数据可能准确输出，发生故障也可能丢失
 *         至少一次（AT-LEAST-ONCE）：利用checkpoint机制，数据至少处理一次，因为两次检查点之间的时间间隔，所以数据可能会处理多次
 *         精确一次（EXACTLY-ONCE）：利用checkpoint机制实现端到端精确一次，使用起来有限制，对Source和Sink条件要求比较高
 *     端到端精确一次
 *         首先保证Source可以重放数据，或者说可以重制读取偏移量，checkpoint之后有些数据需要被重新读取处理，Source保证at-least-once
 *         其次是利用Sink端实现exactly-once
 *             第一是幂等写入，数据重新处理对结果没有影响，例如HashMap，无论将同一条数据更新多少次结果都是一样的，但是输出端会出现短暂的结果不一致，因为结果重新输出同样的数据就会被再次输出一遍，但整体结果是正确的
 *             第二是事务写入，数据处理完先不提交，等到checkpoint成功后再提交，否则回滚
 *                 预写日志（WAL）：先把数据作为log保存起来，等到checkpoint完成后将结果一次性写入外部系统，但这就有些类似于批处理模式，而且存在多次写入操作，写入失败时的操作也很难处理
 *                 两阶段提交（2PC）：数据处理完成先预提交，checkpoint完成后再正式提交，需要输出数据源提供事务支持
 *                     当第一条数据进来或者收到Barrier时，Sink端就会启动一个事务
 *                     接下来收到的所有数据都通过这个事务写入外部系统，由于事务没有提交，所以即使写入外部系统也暂时不可用
 *                     当Sink任务收到JobManage完成checkpoint的通知时，正式提交事务，写入的结果就真正可用了
 *         使用Kafka作为Flink的Source和Sink可以完美的实现exactly-once，实际生产中只需要做一些配置即可
 */
public class ExactlyOnceTest {
}
