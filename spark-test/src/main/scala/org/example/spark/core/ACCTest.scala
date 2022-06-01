package org.example.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

object ACCTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("ACCTest").getOrCreate()
    val sc: SparkContext = spark.sparkContext

//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
//    var total: Int = 0
//    rdd.foreach(total += _)
//    println(total)


//    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
//    val total: LongAccumulator = sc.longAccumulator("total")
//    rdd.foreach({
//      num => {
//        total.add(num)
//      }
//    })
//    println(total.value)

    val rdd: RDD[String] = sc.makeRDD(List("Hello World", "Hello Scala", "Scala Java"))
    val wcAcc = new WordCountAccumulator
    sc.register(wcAcc, "wcAcc")
    rdd.flatMap(_.split(" ")).foreach(
      word => {
        wcAcc.add(word)
      }
    )
    println(wcAcc.value)
  }
  class WordCountAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]] {

    // 定义累加器的数据类型，也是返回的类型
    private val map: mutable.Map[String, Long] = mutable.Map()

    // 累加器是否为初始状态
    override def isZero: Boolean = map.isEmpty

    // 复制累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = new WordCountAccumulator

    // 重置累加器
    override def reset(): Unit = map.clear()

    // 向累加器增加数据（在每个executor中副本的运算规则）
    override def add(word: String): Unit = {
      map(word) = map.getOrElse(word, 0L) + 1L
    }

    // 合并累加器（每个executor运算完成后返回给driver端所有副本的合并规则）
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {

      val map1: mutable.Map[String, Long] = map
      val map2: mutable.Map[String, Long] = other.value

      map2.foreach{
        kv => {
          map1(kv._1) = map1.getOrElse(kv._1, 0L) + kv._2
        }
      }
    }

    // 返回累加器的结果
    override def value: mutable.Map[String, Long] = map
  }
}
