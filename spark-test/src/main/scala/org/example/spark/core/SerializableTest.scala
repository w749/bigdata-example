package org.example.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerializableTest {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    /*
    * 从计算的角度, 算子以外的代码都是在 Driver 端执行, 算子里面的代码都是在 Executor 端执行。那么在 scala 的函数式编程中，
    * 就会导致算子内经常会用到算子外的数据，这样就 形成了闭包的效果，如果使用的算子外的数据未经序列化，就意味着无法传值给 Executor 端执行，
    * 就会发生错误，所以需要在执行任务计算前，检测闭包内的对象是否可以进行序列化，这个操作我们称之为闭包检测。
    * */
    val rdd: RDD[String] = sc.makeRDD(List("Hello", "PHP", "EUB", "Hi"))
    val search = new Search("H")
    search.match01(rdd).collect().foreach(println)  // 需要extends序列化
    search.match02(rdd).collect().foreach(println)  // 可以直接运行


  }
  // 类的构造属性就是类的属性，构造参数需要进行闭包监测，就是对类进行闭包检测
  class Search(str: String) extends Serializable {
    def query(s: String): Boolean = s.contains(str)  // 这里的str其实是类的属性，也就是this.str
    def match01(rdd: RDD[String]): RDD[String] = rdd.filter(query)
    def match02(rdd: RDD[String]): RDD[String] = {
      val ss: String = str  // 但若是将构造函数的参数传递给另外一个字符串参数就可以运行了，因为字符串是经过序列化的
      rdd.filter(_.contains(ss))
    }
  }
}
