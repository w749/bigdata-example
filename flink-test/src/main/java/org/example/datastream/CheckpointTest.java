package org.example.datastream;

import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.example.util.SetUp;
import org.junit.Test;

import java.time.Duration;

/**
 * Checkpoint检查点
 *     checkpoint的定期触发和故障恢复都由JobManager参与
 *     周期性的保存而不是每条数据都去保存一次；并不是时间到了就立马保存，而是等到所有子任务都处理到一个相同输入数据的时候，将它们的状态保存下来
 *     数据源必须支持重放数据，这一条数据处理完保存checkpoint恢复后则可以直接执行下一条数据，如果两次checkpoint保存期间恢复，则需要从上一次checkpoint重新放入数据重新计算
 *     Flink保存checkpoint其实是对执行流程中的状态进行保存，比如Source中的数据偏移量，keyBy后sum聚合中的按键分区状态，需要checkpoint时把这两个状态保存起来，类似map这种转换操作不需要状态则不需要存储
 *     设置检查点->检查点保存（保存状态）->发生故障->重启任务->恢复最近的检查点状态->请求数据源重放数据（有的数据可能已经读进来但是还没处理或者已经处理了没checkpoint，就需要重新计算）
 *
 *     检查点算法
 *         检查点分界线（Barrier）：类似于Watermark，由JobManager向Source数据流插入这个标记，它就像一条数据被往下传，它不能超过其他数据也不能被后面的数据超过，从Source的偏移量开始所有处理流程接收到这个标记都会保存自己的状态
 *         分布式快照算法：核心就两个原则，第一是上游向下游多个并行任务发送barrier时要广播发送，
 *           第二是当多个上游任务向同一个下游任务发送barrier时，需要在下游任务执行“分界线对齐”（barrier alignment）操作，也就是需要等到所有并行分区的 barrier 都到齐，才可以开始状态的保存
 *         因为分界线对齐会耗费时长，且上游barrier未到齐时先到的数据会被缓存起来，缓存内容可能会非常多，对性能造成影响。Flink1.11支持不对齐的检查点保存方式，可以将缓冲的数据也保存到检查点，这样在遇到一个barrier时就可以直接启动状态保存了
 *
 * Savepoint保存点
 *     与checkpoint不同的是savepoint是主动触发保存的，用户主动触发savepoint即可保存指定Job的状态为一份快照，随后可以从快照中恢复状态
 *     注意保存点在程序更改的时候依然兼容，前提是状态的拓扑结构和数据类型不变。保存点中状态都是以算子ID-状态名称这样的 key-value 组织起来的，算子ID可以在代码中直接调用 SingleOutputStreamOperator 的.uid()方法来进行指定
 *     创建保存点：bin/flink savepoint :jobId [:targetDirectory]（仅创建保存点）、bin/flink stop --savepointPath [:targetDirectory] :jobId（创建保存点并且停掉flink）
 *     保存点恢复：bin/flink run -s :savepointPath [:runArgs]
 */
public class CheckpointTest implements SetUp {
    @Test
    public void checkpointTest() {
        env.enableCheckpointing(10000L);
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());  // 检查点位置放到内存中
        env.getCheckpointConfig().setCheckpointStorage("hdfs://namenode:40010/flink/checkpoints");  // 检查点位置放到具体的位置
        CheckpointConfig checkpointConfig = new CheckpointConfig();
        checkpointConfig.setAlignmentTimeout(Duration.ofMillis(60000L));  // 超时时间，检查点保存超过这个时间就不保存了，继续后续数据的处理
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);  // 检查点保存模式，精准一次或者至少一次
        // checkpoint保存有延迟，enableCheckpointing只是定义需要保存的时间间隔，并不是实际保存的时间间隔，这个配置就是设置两个checkpoint实际保存的时间间隔不能超过500ms。作用是给数据处理留时间，如果检查点保存延迟太高就会导致两个检查点保存连在一起没时间处理数据
        checkpointConfig.setMinPauseBetweenCheckpoints(500L);
        checkpointConfig.setMaxConcurrentCheckpoints(1);  // 同一时间内允许同时保存检查点的最大个数，setMinPauseBetweenCheckpoints优先级更高
        checkpointConfig.enableUnalignedCheckpoints();  // 启用分界线不对齐模式，开启的条件是模式为AT_LEAST_ONCE且同时保存检查点的个数只能是1
        // 当任务被取消时是否删除检查点内容，默认删除
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        checkpointConfig.setTolerableCheckpointFailureNumber(0);  // 允许检查点保存失败的数量，默认是-1，不允许失败，如果失败那么任务当前也会失败
    }
}
