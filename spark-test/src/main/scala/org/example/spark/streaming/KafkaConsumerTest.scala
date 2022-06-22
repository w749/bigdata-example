package org.example.spark.streaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.junit.Test

import java.sql.Timestamp

class KafkaConsumerTest {
  private val conf = new SparkConf().setMaster("local[4]").setAppName("Kafka")
  private val ssc = new StreamingContext(conf, Seconds(5))
  ssc.checkpoint("file:///Users/mlamp/workspace/my/example/spark-test/output/checkpoint")

  /**
   * 消费Kafka数据
   */
  @Test
  def kafkaTest(): Unit = {
    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "default",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    // 从Kafka消费数据，通过KafkaUtils创建消费策略，定义KV类型，连接属性和topic等属性
    val kafkaDS = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent, // 采集的节点和计算的节点如何匹配，一般都是自动选择
      ConsumerStrategies.Subscribe[String, String](Seq("ssc"), kafkaParams)
    )

    // 结合窗口时间和StreamingContext的batchDuration实现隔十秒统计一次窗口内的数据
    kafkaDS.map(_.value())
      .countByValueAndWindow(Seconds(10), Seconds(10))
      .transform(dd => {
        println("收集数据: " + new Timestamp(System.currentTimeMillis()))
        dd
      }).foreachRDD(_.foreachPartition(_.foreach(println)))

    ssc.start()
    ssc.awaitTermination()
  }
}
