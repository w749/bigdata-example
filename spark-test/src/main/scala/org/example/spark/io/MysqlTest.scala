package org.example.spark.io

import org.apache.spark.SparkContext
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Test

import java.sql.{Date, DriverManager, Timestamp}
import scala.collection.mutable.ListBuffer
import scala.util.Random


/**
 * Spark读写Mysql分别使用RDD和DataFrame
 */
class MysqlTest extends Serializable {
  private val session: SparkSession = SparkSession.builder()
    .master("local")
    .appName("Mysql")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
  import session.implicits._
  // 因为foreach会用到类的成员变量sc，所以类必须支持序列化，同时标记sc不需要序列化
  @transient
  private val sc: SparkContext = session.sparkContext
  private val url = "jdbc:mysql://win:6606/test?characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai"
  private val driver = "com.mysql.cj.jdbc.Driver"
  private val user = "root"
  private val password = "123456"

  /**
   * 使用RDD读取Mysql数据，直接使用JdbcRDD传入参数就可以获取查询语句对应的RDD
   */
  @Test
  def readMysqlRDD(): Unit = {
    val jdbcRDD = new JdbcRDD[TableTest](
      sc,
      () => {
        Class.forName(driver)
        DriverManager.getConnection(url, user, password)
      },
      "select id, name, tm, `timestamp` from test where ? > 0 and ? < 100 limit 10",
      1,
      3,
      1,
      resultSet => TableTest(
        resultSet.getInt(1),
        resultSet.getString(2),
        resultSet.getDate(3),
        resultSet.getTimestamp(4)
      )
    )
    jdbcRDD.foreach(println)
  }

  /**
   * 使用RDD写入到Mysql，利用foreachPartition在每个分区内新建连接然后写入，相比foreach可以减少连接数量
   */
  @Test
  def writeMysqlRDD(): Unit = {
    // 生成数据
    val testRDD = getData(10)

    // 写入数据
    testRDD.foreachPartition(tableTest => {
      Class.forName(driver)
      val connection = DriverManager.getConnection(url, user, password)
      val sql = "replace into test(id, name, tm, timestamp) values(?, ?, ?, ?)"
      val statement = connection.prepareStatement(sql)
      try {
          tableTest.foreach(data => {
            statement.setInt(1, data.id)
            statement.setString(2, data.name)
            statement.setDate(3, data.tm)
            statement.setTimestamp(4, data.timestamp)
            statement.executeUpdate()
            println(data)
          })
      } finally {
        if (connection != null) connection.close()
        if (statement != null) statement.close()
      }
    })
  }

  /**
   * 读取Mysql为DataFrame
   *   注意dbtable和query不可以同时指定
   */
  @Test
  def readMysqlDF(): Unit = {
    session.read
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      //      .option("dbtable", "test")
      .option("query", "select * from test where id < 3")
      .load()
      .show()
  }

  /**
   * DataFrame写入数据到Mysql，不过不像RDD可以实现插入或者更新，更新就需要重新实现或者修改源码增加功能
   */
  @Test
  def writeMysqlDF(): Unit = {
    val testDF = getData(10).toDF()
    testDF.write
      .format("jdbc")
      .mode(SaveMode.Overwrite)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", "test")
      .save()
  }

  /**
   * 生成数据
   * @param num 数据量
   * @return
   */
  def getData(num: Int): RDD[TableTest] = {
    val testBuffer = ListBuffer[TableTest]()
    val nameList = List("Bob", "Alice", "Kris", "Slice", "John")
    var tm: Date = null
    var timestamp: Timestamp = null
    for (i <- 1 to num) {
      tm = new Date(System.currentTimeMillis())
      timestamp = new Timestamp(System.currentTimeMillis())
      testBuffer += TableTest(i, nameList(Random.nextInt(5)), tm, timestamp)
    }
    sc.makeRDD(testBuffer)
  }
}
