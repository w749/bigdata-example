package org.example.spark.cases

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.example.spark.cases.ControlSizePartitioner._

import scala.collection.mutable

/**
 * Spark指定每个分区的输出大小
 *   这种方式必须是输入和输出数据相同未经过过滤或者flat，如果输入输出数据大小不相同可以借助临时目录
 *   主要利用了FileSystem的getContentSummary方法获取到输入数据的大小，计算出输出指定大小的分区所需的分区数量
 *   还有一种最简单的方法就是通过控制split size的目的达到控制每个分区的数据大小
 */
object ControlPartitionSize {
  var sc: SparkContext = _
  var fs: FileSystem = _
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf
      .setAppName("Test")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val input = "src/main/data/input"
    val output1 = "src/main/data/output1"
    val output2 = "src/main/data/output2"
    sc = new SparkContext(sparkConf)
    val hadoopConf = sc.hadoopConfiguration
    fs = FileSystem.get(hadoopConf)

    // 先把数据过滤后输出到临时文件夹，随后再删除临时文件夹
    sc.textFile(input).filter(_.length > 10).saveAsTextFile(output1)
//    restoreData(output1, output2, 100)
//    repartitionData(output1, output2, 100)
    controlSplitSize(output1, output2, 100)
  }

  /**
   * 通过控制split size的目的达到控制每个分区的数据大小
   */
  def controlSplitSize(srcPath: String, dstPath: String, splitSize: Int): Unit = {
    val config = new Configuration()
    config.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR, srcPath)
    config.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MAXSIZE, s"${splitSize * 1024 * 1024}")
    config.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.SPLIT_MINSIZE, s"${splitSize * 1024 * 1024}")

    sc.newAPIHadoopRDD(
      config,
      classOf[org.apache.hadoop.mapreduce.lib.input.TextInputFormat],
      classOf[LongWritable],
      classOf[Text]
    ).mapPartitions(_.map(_._2)).saveAsTextFile(dstPath)
  }

  /**
   * 读取srcPath数据，按指定分区大小重分区后存入dstPath
   */
  def restoreData(srcPath: String, dstPath: String, partitionSize: Int): Unit = {
    val fileLen = fs.getContentSummary(new Path(srcPath)).getLength
    val partition = Math.max(1, (fileLen / (partitionSize * 1024 * 1024)).toInt)
    sc.textFile(srcPath)
      .repartition(partition)
      .saveAsTextFile(dstPath)
  }

  /**
   * 使用自定义分区器重分区控制每个分区的数据大小
   */
  def repartitionData(srcPath: String, dstPath: String, partitionSize: Int): Unit = {
    val fileLen = fs.getContentSummary(new Path(srcPath)).getLength
    val partition = Math.max(1, (fileLen / (partitionSize * 1024 * 1024)).toInt)
    val partitioner = new ControlSizePartitioner(partition, partitionSize)
    sc.textFile(srcPath)
      .mapPartitions(lines => lines.map(line => (line.getBytes.length, line)))
      .partitionBy(partitioner)
      .map(_._2)
      .saveAsTextFile(dstPath)
  }
}

/**
 * 自定义分区器控制每个分区的数据量大小
 */
class ControlSizePartitioner extends Partitioner{
  private var numPartition = 1

  def this(partitions: Int) {
    this
    this.numPartition = partitions
    initMap(numPartition)
  }

  def this(partitions: Int, size: Long) {
    this
    this.numPartition = partitions
    setSize(size)
    initMap(numPartition)
  }

  override def numPartitions: Int = numPartition

  override def getPartition(key: Any): Int = {
    val lineLength = key.asInstanceOf[Integer]
    var currentId = imbalancePartitions.head
    val value = idSizeMap(currentId)
    if (value < maxSize) {
      idSizeMap.update(currentId, value + lineLength)
    } else {
      imbalancePartitions.remove(currentId)
      currentId = imbalancePartitions.head
    }
    currentId
  }
}

object ControlSizePartitioner {
  // 每个分区最大数据大小，单位为Byte数据量
  private var maxSize = 134217728L
  // 存储未达到最大数据大小的分区
  private val imbalancePartitions = mutable.Set.empty[Int]
  // 存储每个分区的数据大小
  private val idSizeMap = mutable.Map.empty[Int, Long]

  // 初始化数据大小
  private def setSize(size: Long): Unit = maxSize = size * 1024 * 1024

  // 初始化imbalancePartitions和idSizeMap
  private def initMap(partitions: Int): Unit = {
    for (key <- 0 until partitions) {
      idSizeMap += key -> 0L
      imbalancePartitions += key
    }
  }
}
