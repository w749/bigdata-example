package org.example.spark.streaming.exercise

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.{Properties, Random}

/**
 * 调用produce传入server和topic生成数据
 */
class RecordKafkaProducer(server: String, topic: String) {
  private val conf = new Properties()
  conf.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server)
  conf.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
  conf.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

  private val producer = new KafkaProducer[String, String](conf)
  private val userList = Array("ZhangSan", "LiSi", "Wa", "Bob", "Cris", "Lisa")

  /**
   * 每一秒生成一条数据发送到Kafka
   */
  def produce(): Unit = {
    var id = 0
    while (true) {
      id += 1
      val message = generate(id).toString()
      println(message)
      producer.send(new ProducerRecord[String, String](topic, message))
      Thread.sleep(1000)
    }
  }

  /**
   * 生成所需数据
   */
  def generate(id: Int): RecordRow = {
    RecordRow(id, userList(new Random().nextInt(6)), System.currentTimeMillis())
  }

}
