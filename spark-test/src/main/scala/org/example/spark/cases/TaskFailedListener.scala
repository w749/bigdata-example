package org.example.spark.cases

import org.apache.spark.{ExceptionFailure, SparkConf, SparkContext, TaskFailedReason, TaskKilled}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

import javax.xml.bind.DatatypeConverter

object TaskFailedListener {
  var sc: SparkContext = _
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("Test")
    sc = new SparkContext(conf)
    sc.setLogLevel("INFO")
    val listener = new TaskFailedListener()
    sc.addSparkListener(listener)  // 注册Listener

    sc.makeRDD(Seq("Y2xvdWQ5LmZvb3d3LmNvbS9zZXJ2aWNlL0Nsb3VkU2VydmljZS5MTA1JnRhc2tpZD020DM40Tg1NjU20DExNzcx0TE2JnRva2VuaWR4PTEmZW5kPTEmbWtleT0xJlgtSG90LVVybD0yMTc4MA=="))
      .map(DatatypeConverter.parseBase64Binary)
      .collect()
  }
}

class TaskFailedListener extends SparkListener with Logging {
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val info = taskEnd.taskInfo
    if (info != null && taskEnd.stageAttemptId != -1) {
      val errorMassage: Option[String] = taskEnd.reason match {
        case kill: TaskKilled => Some(kill.toErrorString)
        case e: ExceptionFailure => Some(e.toErrorString)
        case e: TaskFailedReason => Some(e.toErrorString)
        case _ => None
      }
      if (errorMassage.isDefined) {
        val error = s"TaskId: ${info.taskId}\nHOST: ${info.host}\nExecutorId: ${info.executorId}\nErrorMassage: ${errorMassage.get}"
        println(error)  // 可以实现发送邮件等操作
      } else {
        logInfo("No Error")
      }
    }
  }
}
