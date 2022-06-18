package org.example.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SparkHive {
  def main(args: Array[String]): Unit = {

    // TODO Spark连接Hive
    /*
    * 1. 将hive-site.xml放在classpath下（resources），并确保它在target/classes下出现，若没有复制过去
    * 2. SparkSession添加enableHiveSupport支持
    * 3. 使用spark.sql写SQL语句
    * */
    System.setProperty("HADOOP_USER_NAME", "root")  // 防止没有访问权限
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkHive")
    // 一定要打开Hive支持-enableHiveSupport
    val spark: SparkSession = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
//      .config("spark.sql.warehouse.dir", "hdfs://node01:9000/warehouse")
      .getOrCreate()

    // TODO 1. 建库建表导入数据
    spark.sql(
      """
        |CREATE DATABASE IF NOT EXISTS spark_test
        |""".stripMargin)
    spark.sql(
      """
        |USE spark_test
        |""".stripMargin)
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `user_visit_action`(
        |  `date` string,
        |  `user_id` bigint,
        |  `session_id` string,
        |  `page_id` bigint,
        |  `action_time` string,
        |  `search_keyword` string,
        |  `click_category_id` bigint,
        |  `click_product_id` bigint,
        |  `order_category_ids` string,
        |  `order_product_ids` string,
        |  `pay_category_ids` string,
        |  `pay_product_ids` string,
        |  `city_id` bigint)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql(
      """
        |load data local inpath 'data/user_visit_action.txt' into table user_visit_action;
        |""".stripMargin)
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `product_info`(
        |  `product_id` bigint,
        |  `product_name` string,
        |  `extend_info` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql(
      """
        |load data local inpath 'data/product_info.txt' into table product_info
        |""".stripMargin)
    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS `city_info`(
        |  `city_id` bigint,
        |  `city_name` string,
        |  `area` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)
    spark.sql(
      """
        |load data local inpath 'data/city_info.txt' into table city_info
        |""".stripMargin)

    // TODO 2. 计算各个区域前三大热门商品，并备注上每个商品在主要城市中的分布比例，超过两个城市用其他显示。
    /*
    * 1. 首先写SQL将其他结果先输出，不处理city这一行，中间需要用到开窗函数取每个大区的前三大热门记录
    * 2. 注册UDAF函数处理city字段
    *   2.1 这一步操作需要加在group by大区的时候处理，不用考虑大区，因为函数的操作就是在每个大区之下，总count就是每个大区的count
    *   2.2 重点在Buffer操作，需要统计每个大区的总点击数和每个城市的点击数，选择样例类自定义数据结构来存储(总点击数,Map(城市,城市点击数))
    *   2.3 初始值不用说，总点击次数是0，Map直接为空
    *   2.4 接下来是Buffer内操作，每条记录更新一次总点击数，更新城市的点击数使用Map下的getOrElse进行自增
    *   2.5 然后是Buffer合并，总点击次数直接相加给Buff1，将Buff2的城市点击数遍历合并给Buff1并返回Buff1
    *   2.6 最后是处理合并后的Buffer，新建一个可变数组用来接收最终的结果，然后对Buffer内的Map转List之后按点击量排序取前两个城市
    *       接下来遍历这两个城市，将它按格式插入最终的可变数组中，最后处理城市数量大于2的并返回可变数组mkString按逗号拼接的字符串
    * */
//    spark.udf.register("remarkCity", functions.udaf(new RemarkCity()))  // 要在spark3.0.0以上使用
    spark.sql(
      """
        |select city.area, product.product_name, count(click.city_id) click_count, remarkCity(city_name) remark_city
        |from user_visit_action click
        |    left join city_info city
        |        on click.city_id = city.city_id
        |    left join product_info product
        |        on click.click_product_id = product.product_id
        |where click.click_product_id != -1
        |group by city.area, product.product_name
        |""".stripMargin).createOrReplaceTempView("tmp01")
    spark.sql(
      """
        |select *, rank() over(partition by area order by click_count) rank
        |from tmp01
        |""".stripMargin).createOrReplaceTempView("tmp02")
    spark.sql(
      """
        |select * from tmp02 where rank <= 3
        |""".stripMargin).show(truncate=false)

    spark.stop()
  }
  /*
  * 用来存储Buffer值，参数分别是（总点击数，Map（城市，城市点击数））
  * */
  case class CityBuffer(var total: Long, var cityMap: mutable.Map[String, Long])
  /*
  * IN：String-城市名
  * Buffer：样例类-【totalClick，Map（city，cityClick）】
  * OUT：String
  * */
  class RemarkCity extends Aggregator[String, CityBuffer, String] {
    override def zero: CityBuffer = {
      CityBuffer(0L, mutable.Map[String, Long]())
    }

    override def reduce(b: CityBuffer, a: String): CityBuffer = {
      b.total += 1  // 每条记录代表一次点击，所以自增
      val cityCount: Long = b.cityMap.getOrElse(a, 0L) + 1  // Map中获取城市对应的点击数，若存在自增，不存在默认为0再自增
      b.cityMap.update(a, cityCount)  // 更新城市的点击数
      b
    }

    override def merge(b1: CityBuffer, b2: CityBuffer): CityBuffer = {
      b1.total += b2.total  // 合并分区，总点击数直接求和

      val buff1: mutable.Map[String, Long] = b1.cityMap
      val buff2: mutable.Map[String, Long] = b2.cityMap

      buff2.foreach {  // 遍历每个buff2中的城市，对buff2进行更新，存在就加上buff2对应的点击数，不存在默认为0再加上buff2对应的点击数
        case (city, cnt) => {
          val buff1CityCount: Long = buff1.getOrElse(city, 0L) + cnt
          buff1.update(city, buff1CityCount)
        }
      }

      b1.cityMap = buff1
      b1
    }

    override def finish(reduction: CityBuffer): String = {
      val cityRemark: ListBuffer[String] = ListBuffer[String]()  // 存储最终的结果，之所以可变列表因为后面需要append

      val total: Long = reduction.total
      val cityMap: mutable.Map[String, Long] = reduction.cityMap

      val cityMapSort: List[(String, Long)] = cityMap.toList.sortWith(_._2 > _._2).take(2)  // 将Map转换为List排序取前两个
      val hasMore: Boolean = cityMap.size > 2  // 返回布尔值，若大于2则后续要添加其他
      var totalPersent: Long = 0L  // 前两个城市的persent值的和

      cityMapSort.foreach{  // 遍历排序后的List将它按格式append到最终的可变列表中
        case (city, cnt) => {
          val persent: Long = cnt * 100 / total
          cityRemark.append(s"${city} ${persent}%")
          totalPersent += persent
        }
      }
      if (hasMore) {  // 处理除了前两个城市之外其他的城市
        cityRemark.append(s"其他 ${100 - totalPersent}%")
      }

      cityRemark.mkString(",")
    }

    override def bufferEncoder: Encoder[CityBuffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}
