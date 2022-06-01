package org.example.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.util.Properties

object LoadSave {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("LoadSave")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // TODO 1. load从文件中加载数据
    println("=====1. load从文件中加载数据=====")
//    spark.read.load("123.parquet")  // 官方默认load只可以读取parquet格式的文件

    // 所以一般使用format指定读取哪种类型的文件，因为是按行读取，所以必须保证每行是按json格式存储的
    val df: DataFrame = spark.read.format("json").load("data/user.json")
    spark.read.json("data/user.json")

    // csv文件的读取还可以指定一些配置操作
    spark.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .load("data/csv.csv")

    spark.read.option("sep", ",")
      .option("header", "false")
      .csv("data/csv.csv")
      .toDF("id", "name", "age")

    // TODO 2. save存储数据到文件
    println("=====2. save存储数据到文件=====")
//    df.write.save("123")  // 官方默认save到一个文件夹，文件格式为parquet

    // 所以使用format+save或者直接使用csv或者json
    df.write.format("json").save("output/output01")
    df.write.csv("output/output02")

    df.write.option("sep", ";")
      .option("header", "true")
      .format("csv")
      .save("output/output03")
    df.write.option("sep", ";")
      .option("header", "true")
      .csv("output/output04")

    // 存储模式有几种，append（追加）、overwrite（覆盖）、ignore（有则不写入没有则存储，防止报错）
    df.write.mode("append").json("output/output01")  // 存在两个数据文件
    df.write.mode("overwrite")
      .option("sep", ";")
      .option("header", "true")
      .csv("output/output02")  // 重写以前的数据文件

    // TODO 3. 从MySQL读取数据
    println("=====3. 从MySQL读取数据=====")
    // 通用方法读取数据
    spark.read.format("jdbc")
      .options(Map(
        "url"->"jdbc:mysql://rm-2zem56ps0i81072ssgo.mysql.rds.aliyuncs.com:3306/test",
        "driver"->"com.mysql.cj.jdbc.Driver",
        "user"->"root_user",
        "password"->"root_user",
        "dbtable"->"student"))
      .load()

    // 使用JDBC的方法
    val prop = new Properties()
    prop.setProperty("user", "root_user")
    prop.setProperty("password", "root_user")
    spark.read
      .jdbc("jdbc:mysql://rm-2zem56ps0i81072ssgo.mysql.rds.aliyuncs.com:3306/test", "student", prop)

    // TODO 4. MySQL写入数据
    println("=====4. MySQL写入数据=====")

    val rdd01: RDD[Student] = spark.sparkContext.makeRDD(
      List(Student("5", "123@qq.com"), Student("6", "456@qq.com"))
    )
    val ds01: Dataset[Student] = rdd01.toDS()
    ds01.write.format("jdbc")
      .options(Map(
        "url"->"jdbc:mysql://rm-2zem56ps0i81072ssgo.mysql.rds.aliyuncs.com:3306/test",
        "driver"->"com.mysql.cj.jdbc.Driver",
        "user"->"root_user",
        "password"->"root_user",
        "dbtable"->"person"))
      .mode(SaveMode.Append)
      .save()



    spark.stop()
  }
  case class Student(id: String, email: String)
}
