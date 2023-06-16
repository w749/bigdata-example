package org.example.spark.cases

import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark.files.ignoreCorruptFiles=true
 * Spark读取输入时过滤掉出现IOException的文件，这对于读取文件损坏的压缩文件很有用
 * 在代码中或者提交作业时的参数中指定的可以生效
 * 此参数只对Spark2.1.0版本以上生效
 */
object IgnoreCorruptFiles {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Test")
      .setMaster("local")
      .set("spark.files.ignoreCorruptFiles", "true")
    val sc = new SparkContext(sparkConf)
  }
}
