package org.example.spark.streaming.exercise

import com.alibaba.druid.pool.DruidDataSourceFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, Timestamp}
import java.util.Properties

/**
 * 从Kafka消费数据并存入ClickHouse
 * @param server Kafka bootstrap-server
 * @param topic Kafka topic
 * @param jdbcProps ClickHouse Properties
 */
class RecordToCK(server: String, topic: String, jdbcProps: Properties) extends Serializable {
  private val checkpoint = "file:///Users/mlamp/workspace/my/example/spark-test/output/checkpoint"
  @transient
  private var ssc: StreamingContext = _

  /**
   * 程序入口
   */
  def main(): Unit = {
    ssc = StreamingContext.getOrCreate(checkpoint, () => updateContext())
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 获取StreamingContext
   */
  def updateContext(): StreamingContext = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("Record")
    ssc = new StreamingContext(conf, Seconds(10))
    processing()
    ssc
  }

  /**
   * 业务代码
   */
  def processing(): Unit = {
    // 从Kafka消费数据
    val params = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> server,
      ConsumerConfig.GROUP_ID_CONFIG -> "RecordGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getName
    )
    val recordRow = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Seq(topic), params)
    )

    // 开窗30秒存一次ClickHouse
    recordRow.map(_.value())
      .window(Seconds(30), Seconds(30))
      .foreachRDD(rdd => {
        rdd.foreachPartition(iter => {
          val rows = iter.toList
          if (rows.nonEmpty) {
            val conn = getConnection(jdbcProps)
            val sql = s"insert into table ${jdbcProps.getProperty("table")} values(?, ?, ?)"
            println(s"接收到数据 ${rows.size} 条，存储数据到ClickHouse: " + new Timestamp(System.currentTimeMillis()))
            rows.foreach(insertTable(conn, sql, _))
            conn.close()
          } else {
            println("当前窗口未接收到数据")
          }
        })
      })
  }

  /**
   * 存入数据到ClickHouse
   */
  def insertTable(conn: Connection, sql: String, row: String): Unit = {
    val statement = conn.prepareStatement(sql)
    val record = row.split(",")
    statement.setInt(1, record(0).toInt)
    statement.setString(2, record(1))
    statement.setTimestamp(3, new Timestamp(record(2).toLong))

    statement.executeUpdate()
    statement.close()
  }

  /**
   * 获取SQL Connection
   */
  def getConnection(prop: Properties): Connection = {
    DruidDataSourceFactory.createDataSource(prop).getConnection
  }
}
