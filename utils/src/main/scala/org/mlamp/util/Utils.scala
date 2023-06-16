package org.mlamp.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, FileUtil, Path}

import java.io.{File, UnsupportedEncodingException}
import java.net.URI
import java.nio.charset.{Charset, StandardCharsets}
import java.util.Base64
import java.util.concurrent.TimeUnit
import javax.xml.bind.DatatypeConverter
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.matching.Regex

object Utils {
  /**
   * 递归删除hadoop目录
   */
  def cleanHadoopPath(hadoopConf: Configuration, path: String): Unit = {
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(hadoopConf)
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
    if (day != 0) sb.append(day + " DAY ")
    if (hours != 0) sb.append(hours + " HOUR ")
    if (minutes != 0) sb.append(minutes + " MINUTES ")
    if (seconds != 0) sb.append(seconds + " SECONDS ")
    sb.append(millis + " MS ")
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
    try {
      val base64Data = DatatypeConverter.parseBase64Binary(base64Str)
      str = new String(base64Data, "utf-8")
    }
    catch {
      case e: UnsupportedEncodingException => e.printStackTrace()
      // ArrayIndexOutOfBoundsException、NegativeArraySizeException
      case e: Exception =>
        println(s"Decode string ${base64Str} failed")
        e.printStackTrace()
    }
    str
  }

  /**
   * 将BASE64编码字符解码为字符串，并将解码错误的记录写入到文件中
   */
  def base64Decode(base64Str: String, split: Array[String], errorArray: ArrayBuffer[String]): String = {
    var str = ""
    try {
      val base64Data = DatatypeConverter.parseBase64Binary(base64Str)
      str = new String(base64Data, "utf-8")
    }
    catch {
      case e: UnsupportedEncodingException => e.printStackTrace()
      // ArrayIndexOutOfBoundsException、NegativeArraySizeException
      case e: Exception =>
        val record = split.mkString("|") + "\n"
        println(s"Base64 decode string '${base64Str}' failed, the record is '${record}'")
        errorArray.append(record)
        e.printStackTrace()
    }
    str
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

  /**
   * 获取 FSDataOutputStream
   */
  def getAppendOutputStream(output: String): FSDataOutputStream = {
    val path = new Path(output)
    val configuration = new Configuration()
    val fs = FileSystem.get(configuration)
    if (!fs.exists(path)) fs.create(path) else fs.append(path)
  }

  /**
   * 将Array中的数据写入hdfs
   */
  def writeArray(output: String, arr: ArrayBuffer[String]): Unit = {
    if (arr.nonEmpty) {
      val outputStream = getAppendOutputStream(output)
      arr.foreach(line => outputStream.write(s"${line}\n".getBytes))
      outputStream.close()
    }
  }
}
