package org.example.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object Test {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("Test").setMaster("local[*]")
    val sc = new SparkContext(conf)

    println(sc.makeRDD(List((1, 2), (1, 3), (2, 4))).reduceByKey(_ + _).collect().mkString("Array(", ", ", ")"))
  }
}
