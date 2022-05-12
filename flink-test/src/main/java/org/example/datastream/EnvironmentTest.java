package org.example.datastream;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

public class EnvironmentTest {
    /**
     * 获取环境
     * Flink从1.12.0开始建议流处理和批处理都使用同一套代码
     * 可以在代码中指定runtimeMode也可以在提交时指定execution.runtime-mode设置是按流处理还是批处理提交代码
     * RuntimeExecutionMode有三个可选值：AUTOMATIC（根据数据源是有界还是无界来选择是批处理还是流处理）、BATCH（批处理）、STREAMING（流处理）
     */
    @Test
    public void getEnv() {
        // 获取本地批处理环境
        ExecutionEnvironment.getExecutionEnvironment();
        // 只能获取到本地的环境
        StreamExecutionEnvironment.createLocalEnvironment();
        // 获取远程的环境并指定jar包
        StreamExecutionEnvironment.createRemoteEnvironment("localhost", 7777, "123.jar");
        // Flink自适应，如果是本地运行则使用本地环境，如果打包到远程服务器运行则获取Flink集群环境
        StreamExecutionEnvironment execution = StreamExecutionEnvironment.getExecutionEnvironment();

        execution.setRuntimeMode(RuntimeExecutionMode.BATCH);  // 设置为批处理提交，默认就是流处理
    }
}
