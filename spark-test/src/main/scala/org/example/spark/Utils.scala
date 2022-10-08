package org.example.spark

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream

import java.io.{File, UnsupportedEncodingException}
import java.net.URI
import java.nio.charset.{Charset, StandardCharsets}
import java.util.Base64
import java.util.concurrent.TimeUnit
import javax.xml.bind.DatatypeConverter
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Try
import scala.util.matching.Regex

object Utils {
  /**
   * 递归删除hadoop目录
   */
  def cleanHadoopPath(sc: SparkContext, path: String): Unit = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(hadoopPath)) {
      fs.delete(hadoopPath, true)
    }
  }

  /**
   * 递归删除本地目录
   */
  def cleanLocalPath(filePath: File): Unit = {
    if (filePath.isDirectory) {
      val fileList = filePath.listFiles()
      for (i <- fileList) {
        if (i.isDirectory) {
          cleanLocalPath(i)
        } else {
          i.delete()
        }
      }
    }
    filePath.delete()
  }

  /**
   * 逐行读取本地文本数据为array
   */
  def readLocalFile(path: String): Array[String] = {
    val source = Source.fromFile(path)
    val array = source.getLines().toArray
    source.close()
    array
  }

  /**
   * 返回在给定的array中的正则是否能匹配到str
   */
  def regexMatch(array: Array[Regex], str: String): Boolean = {
    val iterator = array.iterator
    var result = false
    while(!result && iterator.hasNext) {
      val pattern = iterator.next()
      val defined = pattern.findFirstIn(str)
      // scala提取出来的结果可能为None、Some()、Some(xxx)
      if (defined.isDefined) {
        if (defined.get.nonEmpty) {
          // 考虑到scala正则表达式的提取结果需要比对提取出来的数据和regex是否相同
          result = defined.get.equals(replaceRegexSymbol(pattern.toString()))
        }
      }
    }
    result
  }

  /**
   * 根据前两个字符进行分组匹配
   */
  def indexMatch(map: Map[String, Array[String]], str: String): Boolean = {
    if (str.isEmpty) {
      false
    } else {
      val array = map.getOrElse(str.substring(0, if (str.length > 2) 2 else 1), Array[String]())
      if (array.nonEmpty) array.exists(_.equals(str)) else false
    }
  }

  /**
   * 计算开始到结束毫秒数中间的时间
   */
  def timeInterval(start: Long, end: Long): String = {
    val milliseconds = end - start
    val day: Long = TimeUnit.MILLISECONDS.toDays(milliseconds)
    val hours: Long = TimeUnit.MILLISECONDS.toHours(milliseconds) - TimeUnit.DAYS.toHours(TimeUnit.MILLISECONDS.toDays(milliseconds))
    val minutes: Long = TimeUnit.MILLISECONDS.toMinutes(milliseconds) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(milliseconds))
    val seconds: Long = TimeUnit.MILLISECONDS.toSeconds(milliseconds) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(milliseconds))
    val millis: Long = TimeUnit.MILLISECONDS.toMillis(milliseconds) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(milliseconds))

    val sb: StringBuilder = new StringBuilder
    if (day != 0) sb.append(day + "天")
    if (hours != 0) sb.append(hours + "小时")
    if (minutes != 0) sb.append(minutes + "分")
    if (seconds != 0) sb.append(seconds + "秒")
    sb.append(millis + "毫秒")
    sb.toString()
  }

  /**
   * 替换掉无关前缀
   */
  def replacePrefix(str: String): String = {
    str.replace("http://", "")
      .replace("https://", "")
      .replace("www.", "")
  }

  /**
   * 将给定的资源字符串转为regex
   */
  def makeRegex(str: String): String = {
    val str1 = replacePrefix(str)
    var str2 = if (str1.startsWith(".")) "-*" + str1 else if (str1.startsWith("*")) "-" + str1 else str1
    str2 = if (!str2.startsWith("-*") && !str2.startsWith("^")) "^" + str2
      else if (str2.startsWith("^*")) str2.replace("^*", "-*")
      else str2
    str2 = str2.replace(".", "\\.").replace("-*", ".*")
    str2 = if (str2.endsWith("/")) str2 + ".*"
      else if (str2.endsWith("*")) str2.substring(0, str2.length - 1) + ".*"
      else if (!str2.endsWith("$")) str2 + "$"
      else str2
    str2
  }

  /**
   * 替换掉正则中的字符
   */
  def replaceRegexSymbol(regex: String): String = {
    regex.replace("^", "")
      .replace(".*", "")
      .replace("*", "")
      .replace("\\.", ".")
      .replace("$", ".")
  }

  /**
   * 将字符串加密为base64
   */
  def base64Encode(base64Str: String): String = {
    Base64.getEncoder.encodeToString(base64Str.getBytes())
  }

  /**
   * 将BASE64编码字符解码为字符串
   */
  def base64Decode(base64Str: String): String = {
    var str = ""
    val base64Data = DatatypeConverter.parseBase64Binary(base64Str)
    try // byte[]-->String
      str = new String(base64Data, "utf-8")
    catch {
      case e: UnsupportedEncodingException =>
        e.printStackTrace()
    }
    str
  }

  /**
   * 提取tar.gz文件内容
   * @param ps PortableDataStream数据流
   * @param n 缓冲区字节大小
   * @return
   */
  def extractFiles(ps: PortableDataStream, n: Int = 1024): Try[Array[Array[Byte]]] = Try {
    val tar = new TarArchiveInputStream(new GzipCompressorInputStream(ps.open))
    Stream.continually(Option(tar.getNextTarEntry))
      // Read until next exntry is null
      .takeWhile(_.isDefined)
      .flatten
      // Drop directories
      .filter(!_.isDirectory)
      .map(_ => {
        Stream.continually {
          // Read n bytes
          val buffer = Array.fill[Byte](n)(-1)
          val i = tar.read(buffer, 0, n)
          (i, buffer.take(i))
        }
          // Take as long as we've read something
          .takeWhile(_._1 > 0)
          .flatMap(_._2)
          .toArray})
      .toArray
  }

  /**
   * 将字节转为字符串
   */
  def decode(charset: Charset = StandardCharsets.UTF_8)(bytes: Array[Byte]) = new String(bytes, StandardCharsets.UTF_8)

  /**
   * 根据sep将传入的其他参数连接起来返回String
   */
  def stringBuilder(sep: String, strArray: String*): String = {
    strArray.mkString(sep)
  }

  /**
   * 在HDFS拷贝文件或目录
   */
  def hdfsCopyFile(fs: FileSystem, src: String, dst: String, hadoopConf: Configuration, deleteDst: Boolean = true): Unit = {
    FileUtil.copy(fs, new Path(src), fs, new Path(dst), deleteDst, hadoopConf)
  }

  /**
   * 递归获取文件
   * @param arrayBuffer 存储文件的array，也是最终返回值
   * @param fs FileSystem
   * @param path 路径
   * @param prefix 筛选包含指定前缀的文件，默认全部返回
   * @return
   */
  def listAllFiles(arrayBuffer: ArrayBuffer[URI], fs: FileSystem, path: String, prefix: String = ""): ArrayBuffer[URI] = {
    val listPath = FileUtil.stat2Paths(fs.listStatus(new Path(path)))
    listPath.foreach(path => {
      if (fs.getFileStatus(path).isDirectory) {
        listAllFiles(arrayBuffer, fs, path.toUri.getPath)
      } else {
        arrayBuffer.append(path.toUri)
      }
    })
    if (prefix.nonEmpty) {
      arrayBuffer.filter(new Path(_).getName.startsWith(prefix))
    } else {
      arrayBuffer
    }
  }
}
