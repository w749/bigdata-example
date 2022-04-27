package org.mlamp.hbase

import org.apache.hadoop.hbase.client.Connection
import org.apache.spark.sql.SparkSession
object Main {
  def main(args: Array[String]): Unit = {
//    val connection = HBaseConnection("hbase-site.xml")
    val connection = HBaseConnection(args(0))
    val session = SparkSession.builder().getOrCreate()
    import session.implicits._

    //    hbaseTest(connection)
    val con = HBaseUtil.build(connection)
    con.readAsDf(session, "test:test", "a", List("rowkey", "name", "age"))
//    val data = session.sparkContext.makeRDD(List(("1", "q"), ("2", "w"), ("3", "e"))).toDF("id", "name")
//    util.writeFromDf(session, data, "test:test", "id", "a")
    con.getData("test:test")

    connection.close()
    session.close()
  }
  def hbaseTest(connection: Connection): Unit = {
      val util = HBaseUtil.build(connection)
      util.createTable("test", List("a"))
      util.putData("test", "1", "a", "name", "dd")
      util.putData("test", "2", "a", "name", "Bob")
      util.putData("test", "3", "a", "name", "Alice")
      util.getData("test", "2", cfColumn = Map("a" -> "name"))  // get
      util.getData("test")  // scan
  }

}
