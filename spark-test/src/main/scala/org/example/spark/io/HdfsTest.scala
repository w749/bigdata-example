package org.example.spark.io

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.BZip2Codec
import org.apache.hadoop.io.{LongWritable, SequenceFile, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataTypes
//import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.Test

class HdfsTest {
  private val session: SparkSession = SparkSession.builder()
    .master("local")
    .appName("ClickHouse")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
  import session.implicits._
  private val sc: SparkContext = session.sparkContext

  /**
   * 从Hdfs读取文件到RDD
   *   对于压缩文件可以直接读
   */
  @Test
  def readHdfsRDD(): Unit = {
    // textFile直接读取内容
//    sc.textFile("hdfs://localhost:50070/tmp/test.csv",1).foreach(println)
    // hadoopFile类似于MapReduce，读取过来是KV形式，带有偏移量，TextInputFormat用的是mapred下的
//    sc.hadoopFile[LongWritable, Text, TextInputFormat]("hdfs://localhost:50070/tmp/test.csv").foreach(println)
    // 新API的hadoopFile，用的是mapreduce下的TextInputFormat
    sc.newAPIHadoopFile[LongWritable, Text, TextInputFormat]("hdfs://localhost:50070/tmp/test.csv").foreach(println)
  }

  /**
   * 写入RDD到HDFS
   */
  @Test
  def writeHdfsRDD(): Unit = {
    val path = "hdfs://localhost:50070/tmp/write01"
    val data = cleanAndGetData(path)
    data.saveAsTextFile(path, classOf[BZip2Codec])  // 默认不压缩，可选hdfs支持的压缩方式
  }

  /**
   * 从HDFS读取文件为DataFrame
   */
  @Test
  def readHdfsDF(): Unit = {
    session.read
      .option("sep", ",")
      .option("quote", "")
      .csv("hdfs://localhost:50070/tmp/test.csv")
      .toDF("id", "name", "tm", "timestamp")
      .withColumn("id", col("id").cast(DataTypes.IntegerType))
      .as[TableTest]
      .show()
  }

  /**
   * 写入数据到HDFS
   *   如果需要压缩直接对conf进行配置，可以点进saveAsTextFile查看saveAsHadoopFile方法配置方式，默认是没有压缩的
   */
  @Test
  def writeHdfsDF(): Unit = {
    val conf = session.conf
    conf.set("mapreduce.output.fileoutputformat.compress", "true");
    conf.set("mapreduce.output.fileoutputformat.compress.type", SequenceFile.CompressionType.BLOCK.toString);
    conf.set("mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
    conf.set("mapreduce.map.output.compress", "true");
    conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");

    val path = "hdfs://localhost:50070/tmp/write02"
    val data = cleanAndGetData(path).toDF()
    data.write.csv(path)
  }

  /**
   * 自定义分区器写入Hdfs，也可以使用repartition，必须是KV的数据形式才可以自定义分区
   */
  @Test
  def writeHdfsPartition(): Unit = {
    val path = "hdfs://localhost:50070/tmp/write03"
    val data = cleanAndGetData(path)
      .map(dd => (dd.split("\t")(0), dd))
      .partitionBy(new MyPartition(3))
      .toDF()
    data.write.csv(path)
  }

  /**
   * 清理Hdfs目录，生成数据
   */
  def cleanAndGetData(p: String): RDD[String] = {
    val path = new Path(p)
    val fs = path.getFileSystem(sc.hadoopConfiguration)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    fs.close()

    val mysqlTest = new MysqlTest()
    mysqlTest.getData(10).map(test => Array(test.id, test.name, test.tm, test.timestamp).mkString("\t"))
  }
}

/**
 * 自定义分区器，直接取余数
 */
class MyPartition(num: Int) extends Partitioner {
  private val cnt = num
  // 返回分区数量
  override def numPartitions: Int = {
    cnt
  }

  // 根据传入的key判断数据存储到哪个分区
  override def getPartition(key: Any): Int = {
    val numPartition = Integer.parseInt(key.toString)
    if (cnt == 0) {
      throw new IllegalArgumentException("分区数量不可以为0")
    } else {
      numPartition % cnt
    }
  }
}
