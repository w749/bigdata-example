package org.mlamp.util

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

class HDFSUtils(val session: SparkSession) {
  private val hdfs = FileSystem.get(session.sessionState.newHadoopConf)

  /**
   * 如果路径存在就删除
   */
  def delHdfsPath(path: String): Unit = {
    val hdfsPath = new Path(path)
    if (hdfs.exists(hdfsPath)) hdfs.delete(hdfsPath, true)
  }

  /**
   * 如果路径不存在则创建目标路径
   */
  def createHdfsPath(path: String): Unit = {
    val hdfsPath = new Path(path)
    if (!hdfs.exists(hdfsPath)) hdfs.create(hdfsPath)
  }

  /**
   * put本地数据到HDFS
   */
  def putData(localPath: String, hdfsPath: String): Unit = {
    hdfs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath))
  }
}
