package org.example.spark.exercise

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    // 调度任务，发送任务
    val socket1: Socket = new Socket("localhost", 9999)
    val socket2: Socket = new Socket("localhost", 8888)
    val maskData = new Datas()  // 包含数据和逻辑

    // Executor01
    val out1: OutputStream = socket1.getOutputStream
    val objOut1: ObjectOutputStream = new ObjectOutputStream(out1)
    val subData1 = new SubTask()
    subData1.data = maskData.data.take(2)  // 将数据中的前两项给subtask1
    subData1.calculate = maskData.calculate  // 将逻辑给subtask1
    objOut1.writeObject(subData1)
    objOut1.flush()
    objOut1.close()
    socket1.close()

    // Executor02
    val out2: OutputStream = socket2.getOutputStream
    val objOut2: ObjectOutputStream = new ObjectOutputStream(out2)
    val subData2 = new SubTask()
    subData2.data = maskData.data.takeRight(2)  // 将数据中的前两项给subtask1
    subData2.calculate = maskData.calculate  // 将逻辑给subtask1
    objOut2.writeObject(subData2)
    objOut2.flush()
    objOut2.close()
    socket2.close()

    println("客户端数据发送完毕")


  }
}
