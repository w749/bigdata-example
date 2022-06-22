package org.example.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}
import org.junit.Test

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}

/**
 * 获取StreamingContext
 *     SparkStreaming支持关闭时将context和当前计算的状态保存到checkpoint
 *     获取StreamingContext时可以从checkpoint恢复context和状态，如果上次没有存checkpoint则从给定的函数中获取
 * 优雅的关闭StreamingContext
 *     相比于暴力kill掉线程优雅的关闭会使得计算节点不再接收新的数据等待当前数据操作处理完后关闭
 *     如果需要手动关闭时需要借助第三方存储关闭状态，例如MySQL、Zookeeper或者Redis，当接收到第三方发来的关闭操作后，再执行stop
 *     而且这个关闭操作需要在判断当前StreamingContext是活动状态时才可以关闭
 *     以上的操作都需要一个单独的线程来操作
 * getOrCreate获取之前的状态
 *     从checkpoint获取之前的context和状态，如果没有则获取新的context并执行业务代码
 *     注意一个checkpoint只能保存相同的业务代码状态，如果用到不同的业务代码保存的checkpoint会报has not been initialized
 * 测试：监听4399端口运行WordCount程序并监听4396端口，当在4396端口接收到stop字符串后优雅的关闭程序，尝试重新运行看会不会保存之前的状态
 */
class StartStopTest {
  private val checkpoint = "file:///Users/mlamp/workspace/my/example/spark-test/output/checkpoint"
  // 从指定的checkpoint中获取上一次关闭保存的状态或新建一个连接并把状态保存到指定的checkpoint
  private var ssc: StreamingContext = _

  @Test
  def startStopTest(): Unit = {
    ssc = StreamingContext.getOrCreate(checkpoint, () => {
      val conf = new SparkConf().setMaster("local[4]").setAppName("WordCount")
      ssc = new StreamingContext(conf, Seconds(3))
      ssc.checkpoint(checkpoint)
      wcTest()
      ssc
    })

    // 先启动stop线程，再启动streaming线程
    stop("localhost", 4396)
    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * 业务代码
   */
  def wcTest(): Unit = {
    ssc.socketTextStream("localhost", 4399)
      .flatMap(_.split(" "))
      .map((_, 1))
      .updateStateByKey((seq: Seq[Int], opt: Option[Int]) => Option(opt.getOrElse(0) + seq.sum))
      .print()
  }

  /**
   * 优雅的关闭Spark Streaming
   */
  def stop(host: String, port: Int): Unit = {
    new Thread( new Runnable {
      override def run(): Unit = {
        var str = ""
        val reader = receiveStop(host, port)
        while (true) {
          // 接收数据
          while ((str = reader.readLine()) != null) {
            // 判断诗句并stop
            println("获取到状态: " + str + "，要停止程序请输入stop")
            if (ssc.getState() == StreamingContextState.ACTIVE && str.equalsIgnoreCase("stop")) {
              println("获取到关闭状态: " + str)
              ssc.stop(true, true)
              reader.close()
              System.exit(0)  // 停止当前线程
            }
            Thread.sleep(5000)
            str = reader.readLine()
          }
        }
      }
    }).start()
  }

  /**
   * 接收stop数据
   */
  def receiveStop(host: String, port: Int): BufferedReader = {
    var socket:Socket = null
    try {
      socket = new Socket(host, port)
    } catch {
      case e: ConnectException =>
        println(s"Error connecting to $host:$port", e)
    }

    new BufferedReader(new InputStreamReader(socket.getInputStream))
  }
}
