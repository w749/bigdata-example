package org.example.spark.exercise

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor01 {
  def main(args: Array[String]): Unit = {
    // 模拟分布式计算：Server Executor01
    val socket: ServerSocket = new ServerSocket(9999)
    println("Executor01 服务器已启动，等待传输数据")

    val client: Socket = socket.accept()
    val in: InputStream = client.getInputStream
    val objIn: ObjectInputStream = new ObjectInputStream(in)
    val task1: SubTask = objIn.readObject().asInstanceOf[SubTask]
    val res: List[Int] = task1.func()
    println("Executor01 的计算结果为：" + res)

    in.close()
    client.close()
    socket.close()
  }
}
