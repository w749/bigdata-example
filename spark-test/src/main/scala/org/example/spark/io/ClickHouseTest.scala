package org.example.spark.io

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.junit.Test

import java.sql.DriverManager


/**
 * Spark读写CLickHouse使用DataFrame
 */
class ClickHouseTest {
  private val session: SparkSession = SparkSession.builder()
    .master("local")
    .appName("ClickHouse")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()
  import session.implicits._
  private val url = "jdbc:clickhouse://win:18123/test"
  private val driver = "ru.yandex.clickhouse.ClickHouseDriver"
  private val user = "inuser"
  private val password = "123456"

  /**
   * 创建ClickHouse数据库和表
   */
  @Test
  def createTable(): Unit = {
    val createDbSql = "create database if not exists test"
    val createTbSql = "create table if not exists test.test(id Int32, name String, tm Date, timestamp DateTime) Engine=Log"
    Class.forName(driver)
    val connection = DriverManager.getConnection(url, user, password)
    val statement = connection.createStatement()
    try {
      if (statement.execute(createDbSql)) {
        println("数据库创建成功")
      }
       if (statement.execute(createTbSql)) {
         println("表创建成功")
       }
    } catch {
      case e: Exception => println(e)
    } finally {
      if (connection != null) connection.close()
      if (statement != null) statement.close()
    }
  }

  /**
   * 读取CLickHouse数据，指定Driver参数其他和Mysql一样
   */
  @Test
  def readCK(): Unit = {
    session.read
      .format("jdbc")
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("driver", driver)
      .option("dbtable", "test")
      .option("fetchsize", "100")
      .load().show()
  }

  /**
   * 写入到CLickHouse，指定Driver参数其他和Mysql一样
   */
  @Test
  def writeCK(): Unit = {
    val mysqlTest = new MysqlTest()
    val testDate = mysqlTest.getData(10).toDF()
    testDate.write
      .format("jdbc")
      .mode(SaveMode.Append)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("driver", driver)
      .option("dbtable", "test")
      .option("batchsize", "100")
      .option("isolationLevel", "NONE")
      .option("numPartitions", "1")
      .save()
  }
}
