package org.example.spark.cases

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveInputStream, TarArchiveOutputStream}
import org.apache.commons.compress.compressors.gzip.{GzipCompressorInputStream, GzipCompressorOutputStream}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.io.compress._
import org.apache.spark.{SparkConf, SparkContext}
import org.example.spark.Utils

import java.io.{BufferedReader, InputStream, InputStreamReader, OutputStream}
import java.net.URI
import scala.collection.mutable.ArrayBuffer

/**
 * 自定义CompressionCodec实现输入输出tar.gz压缩格式
 */
object TarGzipCodec {
  var sc: SparkContext = _
  lazy val fs: FileSystem = FileSystem.get(sc.hadoopConfiguration)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Whitelist")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.hadoop.io.compression.codecs", classOf[TarGzipCodec].getName)  // 需要注册

    sc = new SparkContext(sparkConf)
    val input = "data/test-input.tar.gz"
    val output = "data/test-output/tmp"
    Utils.cleanHadoopPath(sc, output)

    sc.textFile(input).saveAsTextFile(output, classOf[TarGzipCodec])

    // 或者通过这种方式，先写出为text file，然后再压缩
    compressTextFile(output, output, 100)
  }


  /**
   * 将指定目录中的text file读到内存中按指定大小再写入该目录并压缩为tar.gz，同时删除原数据
   * @param srcPath 原始数据路径
   * @param dstPath 压缩后数据路径
   * @param size 输出文件大小，单位为M
   * @param delSrcFile 是否删除原数据文件
   */
  def compressTextFile(srcPath: String, dstPath: String, size: Int, delSrcFile: Boolean = true): Unit = {
    val start = System.currentTimeMillis()
    val sizeByte = size * 1024 * 1024
    var count = 0L
    var partPath: Path = null
    val arrayBuffer = new ArrayBuffer[Array[Byte]]()

    if (fs.exists(new Path(srcPath))) {
      val uris = getListFileName(srcPath, "part")  // 获取指定文件前缀的uri
      uris.foreach(uri => {
        partPath = new Path(uri)
        val stream1 = fs.open(partPath)
        val reader = new BufferedReader(new InputStreamReader(stream1))
        var line = reader.readLine()
        while (line != null) {
          // 先将读取的数据存起来
          val bytes = IOUtils.toByteArray(line + "\n")
          arrayBuffer.append(bytes)
          count += bytes.length
          if (count > sizeByte) {
            // 若已读的数据量大小超过指定的大小则存为一个tar.gz文件
            val stream = new TarArchiveOutputStream(new GzipCompressorOutputStream(fs.create(new Path(s"${dstPath}/${partPath.getName}.tar.gz"))))
            val entry = new TarArchiveEntry(s"${partPath.getName}.csv")
            entry.setSize(count)
            stream.putArchiveEntry(entry)
            arrayBuffer.toArray.foreach(stream.write)
            stream.closeArchiveEntry()
            stream.close()
            // 存完后清理
            count = 0L
            arrayBuffer.clear()
          }
          line = reader.readLine()
        }
        if (delSrcFile) Utils.cleanHadoopPath(sc,uri.getPath)  // 删除压缩前的数据
      })
      // read数据读完了但是arrayBuffer中还有一些数据没输出
      if (count != 0) {
        val stream = new TarArchiveOutputStream(new GzipCompressorOutputStream(fs.create(new Path(s"${dstPath}/${partPath.getName}.tar.gz"))))
        val entry = new TarArchiveEntry(s"${partPath.getName}.csv")
        entry.setSize(count)
        stream.putArchiveEntry(entry)
        arrayBuffer.toArray.foreach(stream.write)
        stream.closeArchiveEntry()
        stream.close()
        arrayBuffer.clear()
      }
      val end = System.currentTimeMillis()
      println("压缩共耗时：" + Utils.timeInterval(start, end))
    }
  }


  /**
   * 获取指定目录下指定前缀的文件URI
   */
  def getListFileName(path: String, prefix: String): Array[URI] = {
    val paths = FileUtil.stat2Paths(fs.listStatus(new Path(path)))
      .filter(file => !file.getName.startsWith("_") && file.getName.startsWith(prefix))
    paths.map(_.toUri)
  }
}


/**
 * 自定义读取写入tar.gz压缩文件类，只需实现用到的两个方法
 */
final class TarGzipCodec extends CompressionCodec {
  override def getDefaultExtension: String = ".tar.gz"

  override def createOutputStream(out: OutputStream): CompressionOutputStream = {
    new TarCompressorStream(new TarArchiveOutputStream(new GzipCompressorOutputStream(out)))
  }

  override def createOutputStream(out: OutputStream, compressor: Compressor): CompressionOutputStream = ???

  override def createCompressor(): Compressor = ???

  override def getCompressorType: Class[_ <: Compressor] = ???

  override def createInputStream(in: InputStream): CompressionInputStream = {
    new TarDecompressorStream(new TarArchiveInputStream(new GzipCompressorInputStream(in)))
  }

  override def createInputStream(in: InputStream, decompressor: Decompressor): CompressionInputStream = createInputStream(in)

  override def createDecompressor(): Decompressor = null

  override def getDecompressorType: Class[_ <: Decompressor] = null

  /**
   * Spark压缩输出tar.gz的输出流
   *   tar打包的流程是新建TarArchive输出流，然后需要为当前文件新建一个TarArchiveEntry传入TarArchive输出流
   *   因为TarArchiveEntry的size属性必须指定并且必须和需要输出的字节数量相同，而且运行时无法获取将要写入数据的大小，所以先将数据收集起来，在close时再统一写入文件
   *   数据量大时不推荐使用，可能会造成内存溢出，毕竟一个文件的所有数据都存在ArrayBuffer中
   */
  final class TarCompressorStream(out: TarArchiveOutputStream) extends CompressionOutputStream(out) {
    private var size: Long = 0L
    private val arrayBuffer = new ArrayBuffer[Array[Byte]]()
    private var tarGzipEntry: TarArchiveEntry = _

    override def finish(): Unit = {}

    override def resetState(): Unit = {}

    override def write(b: Int): Unit = {}

    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      size += len
      arrayBuffer.append(b)
//      out.write(b, off, len)
    }

    override def close(): Unit = {
      internalReset()
      // 每个Array[Byte]后都跟有数值为0的Byte，需要过滤掉这些无用数据
      arrayBuffer.toArray.foreach(arr => out.write(arr.filter(_ != 0)))
      out.closeArchiveEntry()
      super.close()
    }

    def internalReset(): Unit = {
      tarGzipEntry = new TarArchiveEntry("file.csv")
      tarGzipEntry.setSize(size)
      out.putArchiveEntry(tarGzipEntry)
    }
  }

  /**
   * Spark读取tar.gz的输入流
   */
  final class TarDecompressorStream(in: TarArchiveInputStream) extends DecompressorStream(in) {
    def updateStream(): Unit = {
      // still have data in stream -> done
      if (in.available() <= 0) {
        // create stream content from following tar elements one by one
        in.getNextTarEntry
      }
    }

    override def read: Int = {
      checkStream()
      updateStream()
      in.read()
    }

    override def read(b: Array[Byte], off: Int, len: Int): Int = {
      checkStream()
      updateStream()
      in.read(b, off, len)
    }

    override def resetState(): Unit = {
    }
  }
}
