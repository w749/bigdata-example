package org.example.datastream;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.example.util.SetUp;
import org.junit.Test;

/**
 * 状态持久化
 *     设置checkpoint
 *     状态后端State Backends
 *         检查点的保存离不开 JobManager 和 TaskManager，以及外部存储系统的协调。在应用进行检查点保存时，首先会由 JobManager 向所有 TaskManager 发出触发检查点的命令；
 *         TaskManger 收到之后，将当前任务的所有状态进行快照保存，持久化到远程的存储介质中；完成之后向 JobManager 返回确认信息。这个过程是分布式的，当 JobManger 收到所有
 *         TaskManager 的返回信息后，就会确认当前检查点成功保存。
 *         在 Flink 中，状态的存储、访问以及维护，都是由一个可插拔的组件决定的，这个组件就叫作状态后端（state backend）。状态后端主要负责两件事：一是本地的状态管理，二是将检查
 *         点（checkpoint）写入远程的持久化存储。
 *         持久化分类
 *             哈希表状态后端（HashMapStateBackend）：默认方式，把状态以键值对的形式保存到内存中，底层是一个HashMap
 *             内嵌 RocksDB 状态后端（EmbeddedRocksDBStateBackend）：RocksDB 是一种内嵌的 key-value 存储介质，可以把数据持久化到本地硬盘
 *             HashMap存储访问快，但是安全性有所欠缺，并且状态较大时内存会不够用；RocksDB保存在磁盘上，足够安全且能够保存更多的状态，但是效率较低
 *             配置时可以在fink-conf.yaml中配置
 *               state.backend: hashmap/rocksdb
 *               state.checkpoints.dir: hdfs://namenode:40010/flink/checkpoints
 */
public class StateStoreTest implements SetUp {
    @Test
    public void checkpointTest() {
        env.enableCheckpointing(10000L);  // 打开checkpoint，flink默认关闭
    }

    @Test
    public void backendTest() {
        env.setStateBackend(new HashMapStateBackend());  // 默认方式HashMapStateBackend
        env.setStateBackend(new EmbeddedRocksDBStateBackend());  // RocksDB方式，需要导入额外的依赖
    }
}
