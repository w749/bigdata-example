package org.example.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSQL {
  def main(args: Array[String]): Unit = {

    // TODO 1. 建立SparkSession连接
    println("=====1. 建立SparkSession连接=====")
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    // SparkSession不允许从外部访问构造函数所以不能new传参，不允许使用伴生对象，但是提供了builder来构造对象并传参
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._  // spark是SparkSession对象名，如果涉及到转换，需要引入转换规则

    // TODO 2. 从json文件读取DataFrame
    println("=====2. 从json文件读取DataFrame=====")
    val df: DataFrame = spark.read.json("data/user.json")

    // TODO 3. 将DataFrame对象转换为视图并使用sql查询
    println("=====3. 将DataFrame对象转换为视图并使用sql查询=====")
    df.createTempView("user")
//    spark.sql("select * from user;").show

    // TODO 4. 使用DSL语句-select
    println("=====4. 使用DSL语句-select=====")
//    df.select("username", "age").show
//    df.select($"age" + 1 as "new_age").show

    // TODO 5. 从Seq创建DataSet
    println("=====5. 从Seq创建DataSet=====")
    val set: Dataset[Int] = Seq(1, 2, 3, 4).toDS
//    set.show

    // TODO 6. RDD、DataFrame、DataSet相互转换
    println("=====6. RDD、DataFrame、DataSet相互转换=====")
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(
      List((1, "Bob", 23), (2, "Alice", 25), (3, "Devi", 26))
    )  // SparkSession底层封装了sparkContext

    // RDD转DF，给字段名就行
    val df01: DataFrame = rdd.toDF("id", "name", "age")
    // RDD转DS，先将元素加上类型
    val ds02: Dataset[User] = rdd.map {
      case (id, name, age) => User(id, name, age)
    }.toDS()
    // RDD转DS，或者先转DF再转DS，不过没必要
    val rs03: Dataset[User] = rdd.toDF("id", "name", "age").as[User]
    // DF转RDD，但是类型和之前稍有不同
    val rdd01: RDD[Row] = df01.rdd
    // DF转DS，给DF加上类型转换为DataSet
    val ds01: Dataset[User] = df01.as[User]
    // DS转DF，直接转
    val df02: DataFrame = ds01.toDF()
    // DS转RDD，直接转
    val rdd02: RDD[User] = ds01.rdd

    println(rdd01.collect().mkString("Array(", ", ", ")"))
    println(rdd02.collect().mkString("Array(", ", ", ")"))

    spark.stop()
  }
  case class User(id: Int, name: String, age:Int)  // 样例类
}
