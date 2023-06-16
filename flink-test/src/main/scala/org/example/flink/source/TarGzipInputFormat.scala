package org.example.flink.source

import org.apache.commons.compress.archivers.{ArchiveInputStream, ArchiveStreamFactory}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.flink.api.common.io.DelimitedInputFormat
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.{FileInputSplit, FileSystem, Path}
import org.apache.flink.util.Preconditions

import java.io.{BufferedInputStream, BufferedReader, IOException, InputStreamReader}
import java.nio.file.FileSystemException
import java.text.{ParseException, SimpleDateFormat}
import java.time.Duration
import scala.util.matching.Regex

/**
 * 读取tar.gz压缩归档文件
 * open -> reachedEnd -> nextRecord
 * 其中open只运行一次，nextRecord通过reachedEnd判断是否需要继续调用
 * 不使用FileInputFormat中获取到的stream，在读取gzip压缩文件时会出错
 * 每个归档文件只允许单线程读取，因为在解压归档文件前无法获取到文件的信息，也就无法分割
 * filterPath方法筛选从指定时间开始读取数据并且跳过已经读取的文件
 */
class TarGzipInputFormat(filePath: Path, configuration: Configuration) extends DelimitedInputFormat[String](filePath, configuration) {
  private val CARRIAGE_RETURN = '\r'.toByte
  private val NEW_LINE = '\n'.toByte
  private val FINISH_SUFFIX = ".ok"
  private var charsetName = "UTF-8"
  private val log: Log = LogFactory.getLog(classOf[TarGzipInputFormat])
  // 匹配输入目录中的日期
  private final val FOLDER_PATTERN_MAP = Map(
    "yyyy-MM-dd/HH" -> "\\d{4}-\\d{2}-\\d{2}/\\d{2}".r,
    "yyyy-MM-dd/HHmm" -> "\\d{4}-\\d{2}-\\d{2}/\\d{4}".r,
    "yyyy-MM-dd" -> "\\d{4}-\\d{2}-\\d{2}".r,
    "yyyyMMdd/HH" -> "\\d{4}\\d{2}\\d{2}/\\d{2}".r,
    "yyyyMMdd/HHmm" -> "\\d{4}\\d{2}\\d{2}/\\d{4}".r,
    "yyyyMMdd" -> "\\d{4}\\d{2}\\d{2}".r
  )
  /**
   * 标识数据是否读取完成
   */
  private var end = false
  private var threadStart = false // 是否已启动线程，一个实例只需启动一次
  private var renameFile = false // 是否重复读取已处理的数据
  private var renameSet = Set[String]()
  private var fs: FileSystem = _
  private var dateFormat: SimpleDateFormat = _
  /**
   * 用来匹配输入目录中的日期时间属性
   */
  private var folderDateRegex: Regex = _
  /**
   * 指定输入数据的开始处理时间
   */
  private var startTimestamp = -1L
  /**
   * 包装FSDataInputStream后的ArchiveInputStream输入流
   */
  private var tarGzipStream: ArchiveInputStream = _
  private var bufferedReader: BufferedReader = _

  def this(filePath: Path, configuration: Configuration, renameFile: Boolean) {
    this(filePath, configuration)
    this.renameFile = renameFile
  }

  def this(filePath: Path, configuration: Configuration, folderDatePattern: String, startTimestamp: Long, renameFile: Boolean = false) {
    this(filePath, configuration)
    Preconditions.checkArgument(FOLDER_PATTERN_MAP.contains(folderDatePattern))
    Preconditions.checkArgument(startTimestamp > 0)
    this.folderDateRegex = FOLDER_PATTERN_MAP(folderDatePattern)
    this.startTimestamp = startTimestamp
    this.dateFormat = new SimpleDateFormat(folderDatePattern)
    this.renameFile = renameFile
  }

  /**
   * 获取由createInputSplits方法生成的FileInputSplit对应的stream
   * 这里自己获取FSDataInputStream并包装为ArchiveInputStream
   * 随后再将ArchiveInputStream包装为BufferedReader便于按行读取数据
   */
  @throws[IOException]
  override def open(fileSplit: FileInputSplit): Unit = {
    super.open(fileSplit)
    val path = currentSplit.getPath
    if (fs == null) fs = path.getFileSystem
    this.stream = fs.open(path)
    filterPath()
    if (renameFile) renameSet += currentSplit.getPath.getPath
    if (!threadStart && renameFile) createRenameThread()
    val in = new GzipCompressorInputStream(new BufferedInputStream(this.stream))
    try {
      tarGzipStream = new ArchiveStreamFactory().createArchiveInputStream("tar", in)
      tarGzipStream.getNextEntry
      bufferedReader = new BufferedReader(new InputStreamReader(tarGzipStream))
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
   * 使用BufferedReader按行读取数据
   * 这里的reuse参数没什么用
   *
   */
  @throws[IOException]
  override def nextRecord(reuse: String): String = {
    val result = bufferedReader.readLine
    if (result != null) result
    else {
      end = true
      null
    }
  }

  override def reachedEnd: Boolean = this.end

  @throws[IOException]
  override def readRecord(reuse: String, bytes: Array[Byte], offset: Int, numBytes: Int): String = {
    var numBytesTmp = numBytes
    if (this.getDelimiter != null && this.getDelimiter.length == 1 && this.getDelimiter.head == NEW_LINE && offset + numBytes >= 1 && bytes(offset + numBytes - 1) == CARRIAGE_RETURN) numBytesTmp -= 1
    new String(bytes, offset, numBytesTmp, this.charsetName)
  }

  override def configure(parameters: Configuration): Unit = {
    super.configure(parameters)
  }

  override def supportsMultiPaths = true

  @throws[IOException]
  override def close(): Unit = {
    super.close()
  }

  def getCharsetName: String = charsetName

  def setCharsetName(charsetName: String): Unit = {
    if (charsetName == null) throw new IllegalArgumentException("Charset must not be null.")
    this.charsetName = charsetName
  }

  /**
   * 根据提供的开始时间和从输入路径中提取日期的正则表达式判断是否读取该文件，不读取直接就跳过；检查是否要跳过已读的文件
   */
  def filterPath(): Unit = {
    var matchDate = ""
    var folderTimestamp = 0L
    val filePath = currentSplit.getPath.getPath
    if (folderDateRegex != null && dateFormat != null && startTimestamp != -1L) {
      folderDateRegex.findFirstMatchIn(filePath) match {
        case matched: Some[Regex.Match] => matchDate = matched.get.toString()
        case _ =>
          log.error(s"使用 ${folderDateRegex.pattern.pattern()} 未匹配到 ${filePath} 中的日期")
          sys.exit(1)
      }
      try {
        folderTimestamp = dateFormat.parse(matchDate).getTime
      } catch {
        case _: ParseException =>
          log.error(s"使用 ${dateFormat.toPattern} 解析 ${matchDate} 失败")
          sys.exit(1)
      }
      end = folderTimestamp < startTimestamp
    }
    if (renameFile && filePath.endsWith(FINISH_SUFFIX)) {
      log.warn(s"${filePath} 包含后缀 ${FINISH_SUFFIX}，跳过该文件")
      end = true
    }
  }

  /**
   * 起一个单独的线程用来修改输入文件的后缀避免重复读取
   * 必须满足有文件待rename、fs不为空以及该文件不跳过的情况下才改名
   */
  def createRenameThread(): Unit = {
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          if (renameSet.nonEmpty && fs != null) {
            var isRename = false
            val currentPath = renameSet.head
            val oldFilePath = new Path(currentPath)
            val newFilePath = new Path(currentPath + FINISH_SUFFIX)

            if (!currentPath.endsWith(FINISH_SUFFIX)) {
              try {
                isRename = fs.rename(oldFilePath, newFilePath)
              } catch {
                case _: FileSystemException => log.warn(s"${currentPath} 正在使用，无法修改名称，待修改文件名称数量: ${renameSet.size}")
              }
            } else isRename = true
            if (isRename) renameSet = renameSet.drop(0)
          }
          Thread.sleep(Duration.ofSeconds(10).toMillis)
        }
      }
    }).start()
    threadStart = true
  }
}
