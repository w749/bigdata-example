package org.example.spark.tracker

import org.apache.spark.sql.SparkSession
import org.apache.spark.status.AppStatusStoreMetadata
import org.apache.spark.{SparkConf, SparkContext, SparkStatusTracker}

object TrackerTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[*]")
    conf.setAppName("TrackerTest")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          println(sc.statusTracker.getActiveJobIds().mkString("Array(", ", ", ")"))
          Thread.sleep(1000)
        }
      }
    }).start()

    sc.makeRDD(Array(1, 3, 5, 7, 9, 5)).map((_, 1)).reduceByKey(_ + _).foreach(println)

    val session = SparkSession.builder().config(conf).getOrCreate()
    session.sql("")
  }

}
