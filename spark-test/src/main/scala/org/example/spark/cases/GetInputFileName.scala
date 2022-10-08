package org.example.spark.cases

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Spark获取输入的数据所属文件名称，如需获取全路径可以在getPath后调用toUri方法再调用getName
 */
object GetInputFileName {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf
      .setAppName("Test")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    val input = "src/main/data/input"
    val sc = new SparkContext(sparkConf)
    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set(org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR, input)

    sc.newAPIHadoopRDD(
      hadoopConf,
      classOf[org.apache.hadoop.mapreduce.lib.input.TextInputFormat],
      classOf[LongWritable],
      classOf[Text]
    ).asInstanceOf[NewHadoopRDD[LongWritable, Text]]  // 只有NewHadoopRDD才有mapPartitionsWithInputSplit方法
      .mapPartitionsWithInputSplit((inputSplit, iterator) => {
        val split = inputSplit.asInstanceOf[FileSplit]
        val name = split.getPath.getName
        iterator.map(value => (name, value._2))
      })
  }
}
