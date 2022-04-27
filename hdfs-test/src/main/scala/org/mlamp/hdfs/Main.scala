package org.mlamp.hdfs

object Main {
  def main(args: Array[String]): Unit = {
    val fSystem = HdfsLogin()
    val hdfs = new HdfsUtil(fSystem)
    println(hdfs.exists("/user/tmpfile/"))
  }
}
