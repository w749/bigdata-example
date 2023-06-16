package org.example.flink.sink

import org.apache.flink.api.common.serialization.BulkWriter
import org.apache.flink.core.fs.FSDataOutputStream
import org.apache.flink.util.Preconditions

import java.nio.charset.StandardCharsets
import java.util.zip.GZIPOutputStream

/**
 *  输出Gzip压缩BulkWriter，如果数据是KV元组类型的只输出value
 */
object GzipBulkWriter {
  final class GzipBulkWriterFactory[T] extends BulkWriter.Factory[T] {
    override def create(out: FSDataOutputStream): BulkWriter[T] = {
      new GzipBulkWriter[T](new GZIPOutputStream(out, true))
    }
  }
}

class GzipBulkWriter[T] extends BulkWriter[T] {
  private val charset = StandardCharsets.UTF_8
  private var stream: GZIPOutputStream = _

  def this(outputStream: GZIPOutputStream) {
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