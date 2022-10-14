package org.example.spark.io

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * 读写Excel文件为DataFrame
 */
object ExcelTest {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = ???
    val peopleSchema = StructType(Array(
      StructField("Name", StringType, nullable = false),
      StructField("Age", DoubleType, nullable = false),
      StructField("Occupation", StringType, nullable = false),
      StructField("Date of birth", StringType, nullable = false)))

    // 读取Excel文件
    spark.read
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'My Sheet'!B3:C35") // 默认："A1"，必须是sheet和位置一起指定
      .option("header", "true") // 是否包含表头
      .option("treatEmptyValuesAsNulls", "false") // 默认: true，空单元格是否转为null
      .option("setErrorCellsToFallbackValues", "true") // 默认: false, 错误将被转为空，若为true则转为当前列类型的零值
      .option("usePlainNumberFormat", "false") // 默认: false, 如果为真，则不使用舍入和科学符号格式化单元格
      .option("inferSchema", "false") // 默认: false
      .option("addColorColumns", "true") // 默认: false
      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // 默认: yyyy-mm-dd hh:mm:ss[.fffffffff]
      .option("maxRowsInMemory", 20) // 默认：None，如果设置将使用流式读取Excel数据（xls文件不支持）
      .option("excerptSize", 10) // 默认: 10，指定推断schema的行数
      .option("workbookPassword", "pass") // 默认：None
      .schema(peopleSchema) // 默认会自己推断，也可以传入自定义schema
      .load("Worktime.xlsx")

    // 写入Excel文件
    val df: DataFrame = ???
    df.write
      .format("com.crealytics.spark.excel")
      .option("dataAddress", "'My Sheet'!B3:C35") // 可以指定不同sheet多次写入，mode需设为append
      .option("header", "true") // 是否包含表头
      .option("dateFormat", "yy-mmm-d") // 默认: yy-m-d h:mm
      .option("timestampFormat", "mm-dd-yyyy hh:mm:ss") // 默认: yyyy-mm-dd hh:mm:ss.000
      .mode("append") // 默认: overwrite.
      .save("Worktime2.xlsx")
  }
}
