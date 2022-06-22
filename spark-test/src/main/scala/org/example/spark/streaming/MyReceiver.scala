package org.example.spark.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import java.util.Random

/**
 * 自定义Receiver
 */
class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY) {
  private var flag = true
  override def onStart(): Unit = {
    new Thread( new Runnable {  // 必须重新启动一个线程生成数据或者收集数据
      override def run(): Unit = {
        while (flag) {
          val str = "采集的数据为: " + new Random().nextInt(10).toString
          store(str)  // 收集数据到StorageLevel
          Thread.sleep(500)
        }
      }
    }).start()
  }

  override def onStop(): Unit = {
    flag = false
  }
}
