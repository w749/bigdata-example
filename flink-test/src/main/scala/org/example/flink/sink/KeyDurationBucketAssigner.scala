package org.example.flink.sink

import org.apache.commons.lang3.StringUtils
import org.apache.flink.core.io.SimpleVersionedSerializer
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer
import org.apache.flink.util.Preconditions

import java.sql.Timestamp
import java.time.format.DateTimeFormatter
import scala.util.Try

/**
 * 根据key和duration依据watermark返回输出目录
 * 例如：key=test；duration=60，返回test/20230101/00
 * 如果要保证数据准确输出并且输出数据中包含时间戳，那么使用数据中的时间戳
 */
class KeyDurationBucketAssigner extends BucketAssigner[(String, String), String] {
  // 分割数据获取时间戳
  private var splitSep = ""
  // 时间戳位置
  private var watermarkLocation = -1
  // 生成目录的分钟间隔
  private var duration = 60
  def this(duration: Int) {
    this()
    this.duration = duration
  }
  def this(duration: Int, splitSep: String, watermarkLocation: Int) {
    this()
    Preconditions.checkArgument(splitSep.nonEmpty)
    Preconditions.checkArgument(watermarkLocation != -1)
    this.duration = duration
    this.splitSep = splitSep
    this.watermarkLocation = watermarkLocation
  }
  override def getBucketId(element: (String, String), context: BucketAssigner.Context): String = {
    // 首先从数据中获取时间戳，如果解析错误就是用当前watermark，为避免没有watermark所以采用当前时间
    val timestamp = {
      val tmp = Try(StringUtils.splitPreserveAllTokens(element._2, splitSep)(watermarkLocation).toLong).getOrElse(context.currentWatermark())
      if (tmp.equals(Long.MaxValue) || tmp.equals(Long.MinValue)) System.currentTimeMillis() else tmp
    }

    if (element._1.isEmpty) fromTimestampToFolder(timestamp, duration) else s"${element._1}/${fromTimestampToFolder(timestamp, duration)}"
  }

  override def getSerializer: SimpleVersionedSerializer[String] = SimpleVersionedStringSerializer.INSTANCE

  /**
   * 从目录刷新间隔判断该时间戳对应的数据应该存到哪个目录中
   */
  def fromTimestampToFolder(timestamp: Long, duration: Int): String = {
    val ts = new Timestamp(timestamp)
    val date = ts.toLocalDateTime.toLocalDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
    val hour = StringUtils.leftPad(ts.getHours.toString, 2, "0")
    var min = ""
    if (duration == 60) {
      min = ""
    } else if (duration > 0 && duration < 60) {
      val cnt = Math.floor(ts.getMinutes / duration).toInt * duration
      min = "/" + StringUtils.leftPad(cnt.toString, 2, "0")
    } else {
      throw new IllegalArgumentException(s"需要传入的时间间隔必须在0-60之间，当前duration: ${duration}")
    }
    s"${date}/${hour}${min}"
  }

}
