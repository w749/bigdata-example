package org.example.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ConnectSpark {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    val textFile: RDD[String] = sc.textFile("data")
    val words: RDD[String] = textFile.flatMap(_.split(" "))
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordAgg: RDD[(String, Int)] = wordGroup.map(kv => (kv._1, kv._2.size))
    val res: Array[(String, Int)] = wordAgg.collect()
    res.foreach(println)
    println("Hello")
    sc.stop()


  }
}
