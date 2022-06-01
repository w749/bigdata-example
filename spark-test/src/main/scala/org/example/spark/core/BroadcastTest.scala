package org.example.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object BroadcastTest {
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("BroadcastTest").getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List( ("a",1), ("b", 2), ("c", 3), ("d", 4) ))
    val list = List( ("a",4), ("b", 5), ("c", 6), ("d", 7) )

    val broadcast: Broadcast[List[(String, Int)]] = sc.broadcast(list)  // 声明广播变量
    rdd1.map {
      case (key, num) => {
        var num2 = 0
        for ((k, v) <- broadcast.value) {  // 使用广播变量
          if (k == key) num2 = v
        }
        (key, (num, num2))
      }
    }.foreach(println)
  }
}
