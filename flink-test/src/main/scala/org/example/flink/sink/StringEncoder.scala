package org.example.flink.sink

import org.apache.flink.api.common.serialization.Encoder

import java.io.OutputStream

/**
 * 继承Encoder实现输出tuple时不输出key
 */
class StringEncoder[IN] extends Encoder[IN]{
  override def encode(element: IN, stream: OutputStream): Unit = {
    element match {
      case tuple: (String, String) =>
        stream.write(tuple._2.getBytes("UTF-8"))
        stream.write('\n')
      case _ =>
        stream.write(element.toString.getBytes("UTF-8"))
        stream.write('\n')
    }
  }
}
