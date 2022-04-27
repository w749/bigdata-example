package org.mlamp.hdfs

import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.{FileSystem, Path}

class HdfsUtil(fileSystem: FileSystem) {
  private val LOG = LogFactory.getLog(classOf[HdfsUtil].getName)
  private val system = fileSystem

  /**
   * 判断文件夹或者文件是否存在
   */
  def exists(path: String): Boolean = {
    system.exists(new Path(path))
  }

  /**
   * 创建HDFS文件夹
   */
  def mkdir(path: String): Unit = {
    try {
      if (exists(path)) {
        LOG.info(s"Path ${path} exists")
      } else {
        system.mkdirs(new Path(path))
      }
    } catch {
      case e: Exception => LOG.info(e + s" Create path ${path} error")
    }
  }

  /**
   * 本地到HDFS
   * @param source 本地数据源
   * @param sink HDFS数据目录
   * @param del 是否删除原数据
   * @param overwrite 是否覆盖HDFS数据
   */
  def put(source: String, sink: String, del: Boolean = false, overwrite: Boolean = false): Unit = {
    system.copyFromLocalFile(del, overwrite, new Path(source), new Path(sink))
  }

  /**
   * HDFS到本地
   * @param source HDFS数据目录
   * @param sink 本地路径
   * @param del 是否删除HDFS数据
   */
  def get(source: String, sink: String, del: Boolean = false): Unit = {
    system.copyToLocalFile(del, new Path(source), new Path(sink), false)
  }

  /**
   * 删除文件夹或文件
   * @param path 待删除路径
   * @param recursion 是否递归删除
   */
  def delete(path: String, recursion: Boolean): Unit = {
    system.delete(new Path(path), recursion)
  }

  def close(): Unit = {
    system.close()
  }
}
