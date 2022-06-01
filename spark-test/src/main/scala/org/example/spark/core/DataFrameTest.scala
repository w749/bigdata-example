package org.example.spark.core

import org.apache.spark.sql.SparkSession

object DataFrameTest {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession
      .builder()
      .appName("Spark SQL")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

//    val df: DataFrame = session.read
//      .format("jdbc")
//      .option("url", "jdbc:mysql://node02:3306/alcon")
//      .option("dbtable", "CouponProduct")
//      .option("user", "root")
//      .option("password", "Wang1024")
//      .load()
//
//    df.show(5)

    session.read  // 读取本地数据
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("data/csv.csv")
      .show()
  }
}
