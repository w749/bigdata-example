package org.example.spark.cases

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{SparkConf, SparkContext}
import org.example.spark.Utils

import java.text.SimpleDateFormat
import java.util.Date

/**
 * 自定义MultipleTextOutputFormat，满足根据key自定义输出目录以及输出文件名称的需求，并且不输出key
 */
object MultipleOutputFormat {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Whitelist")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val output = "data/test-output/tmp"
    Utils.cleanHadoopPath(sc, output)

    sc.makeRDD(Array(("get", "dWE=||1662443766000"), ("get", "dWE=||1662443766000"), ("post", "dWE=||1662443766000"), ("post", "dWE=||1662443766000")))
      .saveAsHadoopFile(
        output,
        classOf[String],
        classOf[String],
        classOf[MultipleOutputFormat]
      )
  }
}


/**
 * 根据数据内容自定义输出目录和文件名
 */
class MultipleOutputFormat extends MultipleTextOutputFormat[String, String] {
  // 重写这个方法将实际的key设为null可以达到不输出key的目的
  override def generateActualKey(key: String, value: String): String = {
    NullWritable.get().asInstanceOf[String]
  }
  override def generateFileNameForKeyValue(key: String, value: String, name: String): String = {
    val split = value.split("\\|")
    val ts = split(2).toLong * 1000
    val date = new SimpleDateFormat("yyyyMMdd").format(new Date(ts))
    val hour = new Date(ts).getHours.toString
    val newName = s"part-${key}-${date}-${hour}-${name.split("-")(1)}"
    if (key == "get") s"get/${newName}" else s"post/${newName}"
  }
}
