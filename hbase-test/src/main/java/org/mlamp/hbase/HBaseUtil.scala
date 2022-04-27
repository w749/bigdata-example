package org.mlamp.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ColumnFamilyDescriptorBuilder, Connection, Get, Put, Result, Scan, TableDescriptorBuilder}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableInputFormat}
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions.GenericHBaseRDDFunctions
import org.apache.hadoop.hbase.spark.{HBaseContext, KeyFamilyQualifier}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory

import java.io.IOException
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object HBaseUtil {
  private val LOG = LoggerFactory.getLogger(HBaseUtil.getClass)
  private var connection: Connection = _

  def build(connection: Connection): this.type = {
    this.connection = connection
    this
  }
  /**
   * 新建HBase表
   * @param tableName 表名
   * @param columnFamily 至少指定一个列簇
   */
  def createTable(tableName: String, columnFamily: List[String]): Unit = {
    val table = connection.getAdmin
    if (table.tableExists(TableName.valueOf(tableName))) {
      LOG.warn(s"表 ${tableName} 已存在")
    } else {
      val cfs = columnFamily.map(cf => ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(cf)).build())
      val desc = TableDescriptorBuilder
        .newBuilder(TableName.valueOf(tableName))
        .setColumnFamilies(cfs.asJavaCollection)
        .build()
      table.createTable(desc)
      LOG.info(s"表 ${tableName} 创建成功")
    }
    table.close()
  }

  /**
   * put数据
   */
  def putData(tableName: String, rowKey: String, cf: String, column: String, value: String): Unit = {
    val table = connection.getTable(TableName.valueOf(tableName))
    val put = new Put(Bytes.toBytes(rowKey))
    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(value))
    table.put(put)
    LOG.info(s"table '${tableName}' rowKey '${rowKey}' insert complete")
    table.close()
  }

  /**
   * get或scan数据
   *
   * @param tableName 表名
   * @param rowKey    rowKey，若指定rowKey则使用get，未指定则使用scan
   * @param cf        列簇，不指定则返回所有列簇内的所有字段，列簇和cfColumn不可以同时指定，指定列簇则返回列簇包含的所有字段
   * @param cfColumn  列簇和字段的Map，返回指定字段或者不同列簇的指定字段时指定
   */
  def getData(tableName: String,
              rowKey: String = "",
              cf: String = "",
              cfColumn: Map[String, String] = Map[String, String]()): Unit = {
    var scan = new Scan()
    val table = connection.getTable(TableName.valueOf(tableName))
    if (cf.nonEmpty && cfColumn.nonEmpty) {
      throw new RuntimeException("限定字段在cfColumn中指定，cf和cfColumn不可同时指定")
    }
    if (rowKey.nonEmpty) {
      val get = new Get(Bytes.toBytes(rowKey))
      scan = new Scan(get)
    }
    if (cf.nonEmpty) {
      scan.addFamily(Bytes.toBytes(cf))
    }
    if (cfColumn.nonEmpty) {
      cfColumn.foreach(col => scan.addColumn(Bytes.toBytes(col._1), Bytes.toBytes(col._2)))
    }

    val scanner = table.getScanner(scan)
    scanner.asScala.toList.foreach {
      result => result.rawCells().foreach {
        cell => {
          val data = MyCell.builder(CellUtil.cloneRow(cell),
            CellUtil.cloneFamily(cell),
            CellUtil.cloneQualifier(cell),
            CellUtil.cloneValue(cell),
            cell.getTimestamp).toString
          println(data)
        }
      }
    }
    scanner.close()
    table.close()
  }

  /**
   * 获取已有HBase表的第一个列族
   */
  def getHBaseColumnFamily(tableName: String): String = {
    val cfList = ListBuffer[String]()
    val table = connection.getTable(TableName.valueOf(tableName))
    table.getDescriptor
      .getColumnFamilies
      .foreach(cf => cfList.append(new String(cf.getName)))
    if (cfList.size != 1) {
      throw new RuntimeException(s"获取到的列族数量为 ${cfList.size}，请检查输入参数或者指定列族")
    }
    table.close()
    cfList.head
  }

  /**
   * Prepare the Put object for bulkload function.
   * @param put The put object.
   * @return Tuple of (KeyFamilyQualifier, bytes of cell value)*/
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def putForLoad(put: Put): Iterator[(KeyFamilyQualifier, Array[Byte])] = {
    val ret: mutable.MutableList[(KeyFamilyQualifier, Array[Byte])] = mutable.MutableList()
    import scala.collection.JavaConversions._
    for (cells <- put.getFamilyCellMap.entrySet().iterator()) {
      val family = cells.getKey
      for (value <- cells.getValue) {
        val kfq = new KeyFamilyQualifier(CellUtil.cloneRow(value), family, CellUtil.cloneQualifier(value))
        ret.+=((kfq, CellUtil.cloneValue(value)))
      }
    }
    ret.iterator
  }

  /**
   * 使用Spark将HBase数据读取为DataFrame
   * @param session SparkSession
   * @param table table名字
   * @param cf 列簇
   * @param fields 字段
   * @return
   */
  def readAsDf(session: SparkSession, table: String, cf: String, fields: List[String]): Unit = {
    // 配置要读取的表和字段
    val hbaseConf: Configuration = connection.getConfiguration
    hbaseConf.set(TableInputFormat.INPUT_TABLE, table)
    fields.filter(field => !field.equalsIgnoreCase("rowkey"))
    hbaseConf.set(TableInputFormat.SCAN_COLUMNS,
      fields
        .filter(field => !field.equalsIgnoreCase("rowkey"))
        .map(field => s"${cf}:${field}")
        .mkString(" "))

    // 将数据读为RDD
    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = session.sparkContext.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    // 解析读出来的数据为目标RDD
//    println(hbaseRDD.count())
    val rowRDD = hbaseRDD.map{row => {
      val values: ListBuffer[String] = new ListBuffer[String]
      val result: Result             = row._2
      val cells = result.rawCells()
      for (cell <- cells) {
        println(Bytes.toString(CellUtil.cloneRow(cell)))
        println(Bytes.toString(CellUtil.cloneValue(cell)))
      }
      for (i <- fields.indices) {
        if (fields(i).equalsIgnoreCase("rowkey")) {
          values += Bytes.toString(result.getRow)
        } else {
          values += Bytes.toString(result.getValue(Bytes.toBytes(cf), Bytes.toBytes(fields(i))))
        }
      }
      Row.fromSeq(values.toList)
    }}

    // 构建DataFrame Schema并创建
    val schema = StructType(
      fields.map(field => DataTypes.createStructField(field, DataTypes.StringType, true)))
    val dataFrame = session.createDataFrame(rowRDD, schema)

    session.close()
    dataFrame.show()

  }

  /**
   * 通过Spark写入DataFrame到HBase
   * @param session SparkSession
   * @param data DataFrame数据
   * @param tableName 表名
   * @param rowKey 以哪一列作为rowKey字段
   * @param cf 写入到哪个列簇
   */
  def writeFromDf(session: SparkSession, data: DataFrame, tableName: String, rowKey: String, cf: String): Unit = {
    val hbaseConf: Configuration = connection.getConfiguration
    var fields = data.columns
    val table = TableName.valueOf(tableName.getBytes())
    val stagingDir = "/tmp/HBaseBulkLoad"  // 不会覆盖，需要手动删除或每次用完后删除

    //去掉rowKey字段
    fields = fields.dropWhile(_ == rowKey)

    val hbaseContext = new HBaseContext(session.sparkContext, hbaseConf)

    //将DataFrame转换bulkLoad需要的RDD格式
    val rddTmp: RDD[Array[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])]] = data.rdd.map(row => {
      val rk = row.getAs[String](rowKey)

      fields.map(field => {
        val value = row.getAs[String](field)
        (Bytes.toBytes(rk), Array((Bytes.toBytes(cf), Bytes.toBytes(field), Bytes.toBytes(value))))
      })
    })
    val rddNew: RDD[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])] = rddTmp.flatMap(array => array)
    rddNew.hbaseBulkLoad(hbaseContext, table,
      t => {
        val rowKey = t._1
        val family:Array[Byte] = t._2(0)._1
        val qualifier = t._2(0)._2
        val value = t._2(0)._3
        val keyFamilyQualifier= new KeyFamilyQualifier(rowKey, family, qualifier)

        Seq((keyFamilyQualifier, value)).iterator
      },
      stagingDir)

    // bulk load start
    val loader = new LoadIncrementalHFiles(hbaseConf)
    loader.doBulkLoad(new Path(stagingDir), connection.getAdmin, connection.getTable(table),
      connection.getRegionLocator(table))

    session.close()
  }
}
