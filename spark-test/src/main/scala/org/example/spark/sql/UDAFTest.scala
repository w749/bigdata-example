package org.example.spark.sql

import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession, TypedColumn, functions}

object UDAFTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("TTT").getOrCreate()
    import spark.implicits._

    val ds: Dataset[Record] = spark
      .sparkContext
      .makeRDD(
        List(Record(1, "Bob", 12.00), Record(1, "Alice", 44.12), Record(1, "John", 23.20),
          Record(2, "Davin", 79.00), Record(2, "Lim", 33.30))).toDS

    ds.createOrReplaceTempView("record")
    spark.udf.register("myudaf01", new MyUDAF01)
//    spark.udf.register("myudaf02", functions.udaf(new MyUDAF02))
    spark.sql(
      """
        |select store, myudaf01(payment), myudaf02(payment) from record group by store
        |""".stripMargin).show(truncate = false)

    val myudaf03: TypedColumn[Record, String] = (new MyUDAF03).toColumn
    ds.select(myudaf03).show(truncate = false)

    spark.stop()
  }
  case class Record(store: Int, name: String, payment: Double)

  /**
   * 弱类型UDAF函数
   */
  class MyUDAF01 extends UserDefinedAggregateFunction {

    override def inputSchema: StructType = {
      StructType(Array(StructField("payment", DoubleType)))
    }

    override def bufferSchema: StructType = {
      StructType(Array(
        StructField("total_user", IntegerType),
        StructField("total_payment", DoubleType)
      ))
    }

    override def dataType: DataType = StringType

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0
      buffer(1) = 0.00
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      buffer(0) = buffer.getInt(0) + 1
      buffer(1) = buffer.getDouble(1) + input.getDouble(0)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
      buffer1(1) = buffer1.getDouble(1) + buffer2.getDouble(1)
    }

    override def evaluate(buffer: Row): Any = "user: " + buffer.getInt(0) + ",payment: " + buffer.getDouble(1)
  }

  case class StoreSummary(var user: Int, var payment: Double)  // 强类型UDAF函数Buffer类型

  /**
   * 强类型UDAF函数（Spark3.0之后）
   */
  class MyUDAF02 extends Aggregator[Double, StoreSummary, String] {
    override def zero: StoreSummary = {
      StoreSummary(0, 0.00)
    }

    override def reduce(b: StoreSummary, a: Double): StoreSummary = {
      b.user += 1
      b.payment += a
      b
    }

    override def merge(b1: StoreSummary, b2: StoreSummary): StoreSummary = {
      b1.user += b2.user
      b1.payment += b2.payment
      b1
    }

    override def finish(reduction: StoreSummary): String = {
      "user: " + reduction.user + ",payment: " + reduction.payment
    }

    override def bufferEncoder: Encoder[StoreSummary] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
  /**
   * 强类型UDAF函数（Spark3.0之前）
   */
  class MyUDAF03 extends Aggregator[Record, StoreSummary, String] {
    override def zero: StoreSummary = {
      StoreSummary(0, 0.00)
    }

    override def reduce(b: StoreSummary, a: Record): StoreSummary = {
      b.user += 1
      b.payment += a.payment
      b
    }

    override def merge(b1: StoreSummary, b2: StoreSummary): StoreSummary = {
      b1.user += b2.user
      b1.payment += b2.payment
      b1
    }

    override def finish(reduction: StoreSummary): String = {
      "user: " + reduction.user + ",payment: " + reduction.payment
    }

    override def bufferEncoder: Encoder[StoreSummary] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }

}
