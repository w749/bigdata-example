package org.example.flink.sink

import org.apache.flink.api.common.serialization.BulkWriter
import org.apache.flink.core.fs.FSDataOutputStream
import org.apache.flink.util.Preconditions

import java.nio.charset.StandardCharsets

/**
 *  输出text格式的BulkWriter，如果数据是KV元组类型的只输出value
 */
object TextBulkWriter {
  final class TextBulkWriterFactory[T] extends BulkWriter.Factory[T] {
    override def create(out: FSDataOutputStream): BulkWriter[T] = {
      new TextBulkWriter[T](out)
    }
  }
}

class TextBulkWriter[T] extends BulkWriter[T] {
  private val charset = StandardCharsets.UTF_8
  private var stream: FSDataOutputStream = _

  def this(outputStream: FSDataOutputStream) {
    this()
    this.stream = Preconditions.checkNotNull(outputStream)
  }

  override def addElement(element: T): Unit = {
    element match {
      case tuple: (String, String) =>
        this.stream.write(s"${tuple._2}\n".getBytes(charset))
      case _ =>
        this.stream.write(String.valueOf(s"${element}\n").getBytes(charset))
    }
  }

  override def flush(): Unit = stream.flush()

  override def finish(): Unit = this.flush()

}
