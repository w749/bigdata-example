package org.example.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object UDFTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("UDF")
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._

    // TODO 1. 自定义UDF
    println("=====1. 自定义UDF======")
    val df: DataFrame = spark.read.json("data/user.json")
    // 注册UDF函数，传入两个参数：函数名和函数内容
    spark.udf.register("addName", (name: String) => "Name: " + name)
    df.createOrReplaceTempView("user")
    spark.sql("select *, addName(username) as new_username from user").show

    // TODO 2. 自定义UDAF函数（弱类型UserDefinedAggregateFunction）
    println("=====2. 自定义UDAF函数-弱类型======")
    val rdd: RDD[(String, Int)] = spark.sparkContext.makeRDD(List(("Bob", 20), ("Alice", 30), ("Devi", 40)))
    val ds: Dataset[User] = rdd.map { case (username, age) => User(username, age) }.toDS()  // 注意导入implicits
    ds.createOrReplaceTempView("newUser")
    spark.udf.register("myAvg01", new MyAvg01)
    spark.sql("select myAvg01(age) as age_avg01 from newUser").show

    // TODO 3. 自定义UDAF函数（强类型UAggregator）
    println("=====3. 自定义UDAF函数-强类型======")
    // Spark3.0之后使用强类型方法，输入直接传入目标字段的类型，sql是弱类型操作，Aggregator是强类型，需要functions.udaf做转换
    spark.udf.register("myAvg02", functions.udaf(new MyAvg02))
    spark.sql("select myAvg02(age) as age_avg02 from newUser").show

    // Spark3.0之前使用强类型方法，在Dataset上做查询，直接将聚合函数转换为查询的列，输入传入Dataset的类型，然后在处理数据时选择目标字段
    val myAvg02: TypedColumn[User, Double] = (new MyAvg03).toColumn
    ds.select(myAvg02).show





    spark.stop()
  }
  // 样例类，用来定义RDD的字段名，也就是类型，转换为DS
  case class User(username: String, age: Int)

  // 使用弱类型UserDefinedAggregateFunction自定义函数
  class MyAvg01 extends UserDefinedAggregateFunction {
    // 聚合函数输入参数的数据类型
    override def inputSchema: StructType = {
      StructType(Array(StructField("age", IntegerType)))
    }

    // 聚合函数缓冲区中值的类型
    override def bufferSchema: StructType = {
      StructType(Array(StructField("sum", LongType), StructField("count", LongType)))
    }

    // 函数返回的数据类型
    override def dataType: DataType = DoubleType

    // 对于相同的输入是否一直返回相同的输出
    override def deterministic: Boolean = true

    // 函数buffer缓冲区初始化，初始值
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L  // sum初始值
      buffer(1) = 0L  // count初始值
    }

    // 更新缓冲区中的数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0) + input.getInt(0)  // 更新buffer中的sum
        buffer(1) = buffer.getLong(1) + 1  // 更新buffer中的count
      }
    }

    // 合并缓冲区（类似于reduce，属于两个元素的合并规则）
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)  // 合并sum
      buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)  // 合并count
    }

    // 计算最终结果
    override def evaluate(buffer: Row): Any = buffer.getLong(0).toDouble / buffer.getLong(1)
  }

  // 样例类，用来定义强类型中的Buffer中的sum和count，这里要显式的定义为var，因为后续变量会改变，默认是val不可变
  case class AgeBuffer(var sum: Long, var count: Long)

  // 使用强类型Aggregator构造UDAF（Spark3.0版本以后）
  class MyAvg02 extends Aggregator[Long, AgeBuffer, Double] {
    override def zero: AgeBuffer = {  // 初始化Buffer中的字段
      AgeBuffer(0L, 0L)
    }

    override def reduce(b: AgeBuffer, a: Long): AgeBuffer = {  // 输入到Buffer的聚合
      b.sum = b.sum + a
      b.count = b.count + 1
      b
    }

    override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {  // 合并Buffer
      b1.sum = b1.sum + b2.sum
      b1.count = b1.count + b2.count
      b1
    }

    override def finish(reduction: AgeBuffer): Double = {  // 最终的计算结果
      reduction.sum / reduction.count
    }

    // Dataset默认编码器，用于序列化，固定写法
    override def bufferEncoder: Encoder[AgeBuffer] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }
  // 使用强类型Aggregator构造UDAF（Spark3.0版本以前）
  class MyAvg03 extends Aggregator[User, AgeBuffer, Double] {
    override def zero: AgeBuffer = {  // 初始化Buffer中的字段
      AgeBuffer(0L, 0L)
    }

    override def reduce(b: AgeBuffer, a: User): AgeBuffer = {  // 输入到Buffer的聚合
      b.sum = b.sum + a.age
      b.count = b.count + 1
      b
    }

    override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {  // 合并Buffer
      b1.sum = b1.sum + b2.sum
      b1.count = b1.count + b2.count
      b1
    }

    override def finish(reduction: AgeBuffer): Double = {  // 最终的计算结果
      reduction.sum / reduction.count
    }

    // Dataset默认编码器，用于序列化，固定写法
    override def bufferEncoder: Encoder[AgeBuffer] = Encoders.product

    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
  }
}
