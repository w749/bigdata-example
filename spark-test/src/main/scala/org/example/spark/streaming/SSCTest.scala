package org.example.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import org.junit.Test

/**
 * SparkStreaming基本操作
 *     WordCount
 *     自定义Receiver
 *     updateStateByKey
 *     transform
 *     无状态join
 *     window
 *     优雅的stop
 *     精准一次消费: https://spark.apache.org/docs/3.1.2/streaming-kafka-0-10-integration.html
 */
class SSCTest {
  private val conf = new SparkConf()
    .setMaster("local[4]")
    .setAppName("WordCount")
    // 单位：毫秒，设置从Kafka拉取数据的超时时间，超时则抛出异常重新启动一个task
    .set("spark.streaming.kafka.consumer.poll.ms", "100000")
    // 控制每秒读取Kafka每个Partition最大消息数(500*3*10=15000)，若Streaming批次为10秒，topic最大分区为3，则每批次最大接收消息数为15000
    .set("spark.streaming.kafka.maxRatePerPartition","500")
    // 开启KryoSerializer序列化
    .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    // 开启反压，默认开启
    .set("spark.streaming.backpressure.enabled","true")
    // 开启推测，防止某节点网络波动或数据倾斜导致处理时间拉长(推测会导致无数据处理的批次，也消耗与上一批次相同的执行时间，但不会超过批次最大时间，可能导致整体处理速度降低)
    .set("spark.speculation","true")
  private val ssc = new StreamingContext(conf, Seconds(3))
  private val ds: DStream[String] = ssc.socketTextStream("localhost", 4399).flatMap(_.split(" "))
  ssc.checkpoint("file:///Users/mlamp/workspace/my/example/spark-test/output/checkpoint")  // 有状态计算时必须指定checkpoint

  /**
   * 利用Socket WordCount
   */
  @Test
  def wcTest(): Unit = {
    ds.map((_, 1))
      .reduceByKey(_ + _)
      .print()

    start()
  }

  /**
   * 自定义Receiver接受数据
   */
  @Test
  def myReceiverTest(): Unit = {
    ssc.receiverStream(new MyReceiver()).print()
    start()
  }

  /**
   * 使用updateStateByKey按key值更新状态
   */
  @Test
  def updateStateByKey(): Unit = {
    ds.map((_, 1))
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => Option(opt.getOrElse(0) + seq.sum))
      .print()

    start()
  }

  /**
   * 将DStream转为RDD来操作，目的是解决DS的有些功能不全或者需要周期性的执行一些操作
   *     运行下面例子可以看到，transform周期性的执行还在Driver端，所以每个窗口都可以输出
   *     但是ds直接map只有在有数据的时候才会执行里面的操作，它是在executor端执行的
   */
  @Test
  def transformTest(): Unit = {
   ds.map((_, 1))
     .transform(rdd => {
       println("transform内周期性地执行")
       rdd.map(rdd => rdd)
     })
     .map(data => {
       println("map内执行")
       data
     }).print()

    start()
  }

  /**
   * DS的无状态join操作底层就是两个窗口内rdd的join操作
   */
  @Test
  def joinTest(): Unit = {
    val ds2 = ssc.socketTextStream("localhost", 4396).flatMap(_.split(" ")).map((_, 2))
    val ds1 = ds.map((_, 1))

    ds1.join(ds2).print()
    start()
  }

  /**
   * Window操作
   *     对采集来的数据进行开窗计算，窗口大小一般是采集周期的整数倍
   *     默认窗口滑动步长是一个采集周期，可以修改使得滑动步长等于窗口周期，就成了滚动窗口
   *     countByWindow: 计算窗口内元素个数
   *     reduceByWindow: 结合window和reduce
   *     reduceByKeyAndWindow: 结合window和reduceByKey
   *     countByValueAndWindow: 计算窗口内每个值的出现次数，返回(value, count)，相当于WordCount
   *     groupByKeyAndWindow: 结合groupByKey和window，将同一窗口内相同key的数据收集到一起做处理
   */
  def windowTest(): Unit = {
    ds.map((_, 1))
      .window(Seconds(6), Seconds(6))
      .reduceByKey(_ + _)
      .print()
    start()
  }

  /**
   * reduceByKeyAndWindow有一个重载方法里支持定义规约滑动过去的采集窗口
   *     第一个参数reduceFunc是处理当前滑动窗口内现有的采集窗口和新增的采集窗口
   *     第二个参数invReduceFunc是处理当前滑动窗口内现有的采集窗口和已经滑过去的采集窗口
   */
  @Test
  def windowTest2(): Unit = {
    ds.map((_, 1))
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y,
        (x: Int, y: Int) => x - y,
        Seconds(9),
        Seconds(3)
      ).print()
    start()
  }

  // 启动
  def start(): Unit = {
    ssc.start()  // 启动采集器
    ssc.awaitTermination()  // 等待采集器的关闭
  }

}
