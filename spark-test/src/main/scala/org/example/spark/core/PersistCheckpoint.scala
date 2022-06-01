package org.example.spark.core

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object PersistCheckpoint {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("PersistCheckpoint")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    sc.setCheckpointDir("output/checkpoint")  // 设置检查点目录，一般放在HDFS

    // TODO 1. 不使用持久化，重复使用RDD时会重新计算一次
    println("=====1. 不使用持久化，重复使用RDD时会重新计算一次=====")
    val rdd: RDD[String] = sc.makeRDD(List("Hello World", "Hello Scala"))
    val mapRDD: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map(kv => {
      println("@@@@@")
      (kv, 1)
    })
    // 因为RDD不存储数据，所以当我们复用RDD的时候它并不会把里面的数据拿过来使用，而是会根据血缘关系重新计算一遍
    println(mapRDD.reduceByKey(_ + _).collect().mkString("Array(", ", ", ")"))
    println("+++++++++++")
    println(mapRDD.groupByKey().collect().mkString("Array(", ", ", ")"))

    // TODO 2. 使用cache和persist持久化
    println()
    println("=====2. 使用cache和persist持久化=====")
    val mapRDD1: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map(kv => {
      println("@@@@@")
      (kv, 1)
    })

    // persist持久化操作会将RDD数据存储在内存或者磁盘用来复用或是保存重要数据，当前作业完成就会清理掉
    mapRDD1.persist(StorageLevel.MEMORY_ONLY)
//    mapRDD1.cache()  // cache其实就是persist传入MEMORY_ONLY参数值

    println(mapRDD1.reduceByKey(_ + _).collect().mkString("Array(", ", ", ")"))
    println("+++++++++++")
    println(mapRDD1.groupByKey().collect().mkString("Array(", ", ", ")"))

    // TODO 3. checkpoint检查点
    println()
    println("=====3. checkpoint检查点=====")
    val mapRDD2: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map(kv => {
      println("@@@@@")
      (kv, 1)
    })

    // checkpoint是将运算数据保存在磁盘或者HDFS，数据并不会随着作业的结束而删除，可以跨作业使用，需要提前设置检查点目录
    // checkpoint为了数据安全会重新启动一个作业用来运算并保存当前RDD，效率会很低，所以一般结合cache使用，直接使用内存中持久化的数据
    mapRDD2.cache()
    mapRDD2.checkpoint()

    println(mapRDD2.reduceByKey(_ + _).collect().mkString("Array(", ", ", ")"))
    println("+++++++++++")
    println(mapRDD2.groupByKey().collect().mkString("Array(", ", ", ")"))

    // TODO 4. cache、persist和checkpoint的区别
    println()
    println("=====4. cache、persist和checkpoint的区别=====")
    /**
     * (1) cache：将RDD数据存储在内存中，以供复用或者保证数据安全，底层调用persist
     *            作业结束数据就会销毁
     *            除了会保存数据还会保存血缘关系，当数据丢失可应根据血缘关系重新计算
     * (2) persist：将RDD数据存储在磁盘或者内存中，有多种存储方式，内存、磁盘、内存和磁盘、磁盘和磁盘副本等
     *            作业结束数据就会销毁
     *            除了会保存数据还会保存血缘关系，当数据丢失可应根据血缘关系重新计算
     * (3) checkpoint：将RDD数据永久保存在磁盘或者HDFS中
     *            作业结束并不会删除数据，可以跨作业调用
     *            并不会保存数据的血缘关系，因为如果跨作业执行产生checkpoint的作业已经结束，无法再根据血缘关系重新计算
     */
    val mapRDD3: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map(kv => {
      println("@@@@@")
      (kv, 1)
    })

    mapRDD3.cache()

    println("cache血缘：\n" + mapRDD3.toDebugString)
    println(mapRDD3.reduceByKey(_ + _).collect().mkString("Array(", ", ", ")"))
    println("+++++++++++")
    println(mapRDD3.toDebugString)
    println(mapRDD3.groupByKey().collect().mkString("Array(", ", ", ")"))

    val mapRDD4: RDD[(String, Int)] = rdd.flatMap(_.split(" ")).map(kv => {
      println("@@@@@")
      (kv, 1)
    })

    mapRDD4.cache()
    mapRDD4.checkpoint()

    println("checkpoint血缘：\n" + mapRDD4.toDebugString)
    println(mapRDD4.reduceByKey(_ + _).collect().mkString("Array(", ", ", ")"))
    println("+++++++++++")
    println(mapRDD4.toDebugString)
    println(mapRDD4.groupByKey().collect().mkString("Array(", ", ", ")"))

    sc.stop()
    spark.stop()
  }
}
