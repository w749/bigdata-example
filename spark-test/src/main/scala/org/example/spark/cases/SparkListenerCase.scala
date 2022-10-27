package org.example.spark.cases

import org.apache.spark.scheduler._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * SparkListener监听器，负责监视Spark作业运行时的状态，可以实现SparkListener收集运行时的状态
 */
object SparkListenerCase {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SparkListener")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    sc.setLogLevel("WARN")

    val listener = new BasicJobCounter("")  // 自定义SparkListener
    sc.addSparkListener(listener)

    sc.makeRDD(Seq(1, 3, 5, 7)).collect()
    println(listener.result)
  }

  class BasicJobCounter(var result: String) extends SparkListener {
    override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
      println("---ApplicationStart---")
      println(applicationStart.appId)
      println(applicationStart.time)
      println(applicationStart.appName)
      println(applicationStart.sparkUser)
      println(applicationStart.driverLogs.mkString(","))
      println("---ApplicationStart---")
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
      println("---StageCompleted---")
      println(stageCompleted.stageInfo.name)
      println(stageCompleted.stageInfo.stageId)
      println(stageCompleted.stageInfo.details)
      println(stageCompleted.stageInfo.numTasks)
      println(stageCompleted.stageInfo.taskMetrics.inputMetrics.recordsRead)
      println(stageCompleted.stageInfo.taskMetrics.jvmGCTime)
      println("---StageCompleted---")
      result = stageCompleted.stageInfo.name
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
      println("---JobEnd---")
      println(jobEnd.jobId)
      println(jobEnd.jobResult.getClass.getName)
      println("---JobEnd---")
    }
  }
}
