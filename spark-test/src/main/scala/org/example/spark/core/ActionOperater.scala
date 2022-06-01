package org.example.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ActionOperater {
  def main(args: Array[String]): Unit = {
    // 所谓的行动算子就是能够触发作业执行的方法

    val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val rdd01: RDD[Int] = sc.makeRDD(List(1, 3, 2, 4, 5, 3))
    val rdd02: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 3), ("b", 2), ("b", 4)))
    // TODO 01. 行动算子-collect（将不同分区的数据按照分区顺序采集到Driver，形成数组）
    println("=====01. 行动算子-collect（将不同分区的数据按照分区顺序采集到Driver，形成数组）=====")
    println(rdd01.collect().mkString("Array(", ", ", ")"))

    // TODO 02. 行动算子-reduce（根据传入的方法对元素进行规约）
    println("=====02. 行动算子-reduce（根据传入的方法对元素进行规约）=====")
    println(rdd01.reduce(_ + _))

    // TODO 03. 行动算子-count（对RDD中的元素计数）
    println("=====03. 行动算子-count（对RDD中的元素计数）=====")
    println(rdd01.count())

    // TODO 04. 行动算子-first（取RDD中的第一个元素）
    println("=====04. 行动算子-first（取RDD中的第一个元素）=====")
    println(rdd01.first())

    // TODO 05. 行动算子-take（从左取n个数据）
    println("=====05. 行动算子-take（从左取n个数据）=====")
    println(rdd01.take(2).mkString("Array(", ", ", ")"))

    // TODO 06. 行动算子-takeOrdered（升序排序后从左取n个数据）
    println("=====06. 行动算子-takeOrdered（升序排序后从左取n个数据）=====")
    println(rdd01.takeOrdered(3)(Ordering.Int.reverse).mkString("Array(", ", ", ")"))

    // TODO 07. 行动算子-aggregate（给一个初始值，进行分区内聚合和分区间聚合）
    println("=====07. 行动算子-aggregate（给一个初始值，进行分区内聚合和分区间聚合）=====")
    // 和aggregateByKey不同的是初始值会在分区内和分区间分别计算，aggregateByKey只会在分区内聚合使用
    println(rdd01.aggregate(10)(_ + _, _ + _))

    // TODO 08. 行动算子-fold（给一个初始值，分区内聚合和分区间聚合相同的聚合）
    println("=====08. 行动算子-fold（给一个初始值，分区内聚合和分区间聚合相同的聚合）=====")
    println(rdd01.fold(10)(_ + _))  // 和aggregate一样，初始值分别参与分区内和分区间聚合

    // TODO 09. 行动算子-countByValue（按值对每个值进行计数）
    println("=====09. 行动算子-countByValue（按值对每个值进行计数）=====")
    println(rdd01.countByValue())

    // TODO 10. 行动算子-countByKey（按key对每个值进行计数）
    println("=====10. 行动算子-countByKey（按key对每个值进行计数）=====")
    println(rdd02.countByKey())

    // TODO 11. 行动算子-foreach（executor中的数据遍历）
    println("=====11. 行动算子-foreach（executor中的数据遍历）=====")
    // 如果直接foreach(println)的话直接从executor中遍历打印数据，顺序可能会打乱，调用RDD中的foreach
    // 而collect().foreach(println)是从Driver中遍历打印，Driver中的数据是collect从executor按顺序收集过来的，调用Scala的foreach
    rdd01.foreach(println)

    sc.stop()
  }
}
