package org.example.spark.cases

import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 使用FileSystem.globStatus方法按文件名过滤文件
 */
object PathFilter {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Test")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)

    val input = "/tmp/*"
    val filterInput = FileSystem.get(sc.hadoopConfiguration).globStatus(new Path(input), new PathFilter {
      override def accept(path: Path): Boolean = {
        // 在此输入正则表达式规则，匹配不到则过滤掉，最终返回负责规则文件组成的数组
        path.toString.matches(".*21[0-9]{2}\\.csv\\.gz$")
      }
    }).map(_.getPath.toString)

    sc.textFile(filterInput.mkString(",")).collect()
  }
}
