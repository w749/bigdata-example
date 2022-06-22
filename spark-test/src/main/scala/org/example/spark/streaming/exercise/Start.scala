package org.example.spark.streaming.exercise

import org.junit.Test

import java.sql.DriverManager
import java.util.Properties

/**
 * Kafka -> Spark Streaming -> ClickHouse
 *     先启动processData再启动generateData
 */
class Start {
  private val server = "localhost:9092"
  private val topic = "ssc"
  private val prop = new Properties()
  prop.setProperty("url", "jdbc:clickhouse://win:18123/record")
  prop.setProperty("username", "root")
  prop.setProperty("password", "123456")
  prop.setProperty("table", "record")
  prop.setProperty("initialSize", "5")
  prop.setProperty("maxActive", "10")
  prop.setProperty("maxWait", "3000")
  prop.setProperty("testWhileIdle", "false")
  prop.setProperty("driverClassName", "ru.yandex.clickhouse.ClickHouseDriver")
  /**
   * 生成测试数据到Kafka
   */
  @Test
  def generateData(): Unit = {
    new RecordKafkaProducer(server, topic).produce()
  }

  /**
   * 从Kafka消费数据并写入到ClickHouse
   */
  @Test
  def processData(): Unit = {
    new RecordToCK(server, topic, prop).main()
  }

  /**
   * 连接ClickHouse测试
   */
  @Test
  def ckTest(): Unit = {
    Class.forName(prop.getProperty("driverClassName"))
    val connection = DriverManager.getConnection(prop.getProperty("url"), prop.getProperty("username"), prop.getProperty("password"))
    println(connection.getSchema)
    connection.close()
  }
}
