//import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration, TableName}
//import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Result, Scan}
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.types.{DataTypes, StructType}
//import org.apache.spark.sql.{DataFrame, Row, SparkSession}
//
//import scala.collection.mutable.ListBuffer
//import scala.collection.parallel.immutable
//
//object Test {
//  def main(args: Array[String]): Unit = {
//    val session = SparkSession
//      .builder()
//      .getOrCreate()
//
//    val host = "192.168.1.52"
//    val port = "24002"
//    val table = "NEST:CETUS_EVENT_NEST_FI651_3_FINANCE_DEV_BILL_INFO"
//    getHbaseSession("192.168.1.52", "24002", "NEST:CETUS_EVENT_NEST_FI651_3_FINANCE_DEV_BILL_INFO")
//    read(session, "0", host, port, table)
//
////    session.read.format("jdbc").option("driver", "org.apache.phoenix.jdbc.PhoenixDriver").option("url", "jdbc:phoenix:192.168.1.52:24002").option("dbtable", "NEST.CETUS_EVENT_NEST_FI651_3_FINANCE_DEV_EMP_JOBDATA").load().show(truncate = false)
//  }
//
//  def getHbaseSession(host: String, port: String, table: String): Unit = {
//    val scanConf = HBaseConfiguration.create()
//    scanConf.set("hbase.zookeeper.quorum", host)
//    scanConf.set("hbase.zookeeper.property.clientPort", port)
//    println("config successful")
//    val connection = ConnectionFactory.createConnection(scanConf).getTable(TableName.valueOf(table))
//    println("connection successful")
//    connection.get(new Get(Bytes.toBytes("corp000000001_cust1640686049006corp000000001_cust"))).getFamilyMap(Bytes.toBytes("0"))
//    val scanner = connection.getScanner(new Scan()).iterator()
//    println("scan successful")
//    while (scanner.hasNext) {
//      val cells = scanner.next().rawCells()
//      println("cell successful")
//      for (cell <- cells) {
//        println("rowkey: " + new String(CellUtil.cloneFamily(cell)) +
//          ", cf: " + new String(CellUtil.cloneQualifier(cell)) +
//          ", value: " + new String(CellUtil.cloneValue(cell)))
//      }
//    }
//  }
//
//  def read(session: SparkSession,
//           columnFamily: String,
//           host: String,
//           port: String,
//           table: String,
//           fields: List[String]): DataFrame = {
//    var cf       = columnFamily
//    val scanConf = HBaseConfiguration.create()
//    scanConf.set("hbase.zookeeper.quorum", host)
//    scanConf.set("hbase.zookeeper.property.clientPort", port)
//
//    // 若未指定列族则获取列族
//    if (cf.isEmpty) {
//      val cfList: ListBuffer[String] = null
//      ConnectionFactory.createConnection(scanConf).getAdmin
//        .getTableDescriptor(TableName.valueOf(table))
//        .getColumnFamilies
//        .foreach(cf => cfList.append(new String(cf.getName)))
//      if (cfList.size != 1) {
//        throw new RuntimeException(s"获取到的列族数量为 ${cfList.size}，请检查输入参数或者指定列族")
//      }
//      cf = cfList.head
//    }
//
//    scanConf.set(TableInputFormat.INPUT_TABLE, table)
//    fields.filter(field => !field.equalsIgnoreCase("rowkey"))
//    scanConf.set(TableInputFormat.SCAN_COLUMNS,
//    fields
//      .filter(field => !field.equalsIgnoreCase("rowkey"))
//      .map(field => s"$columnFamily:$field")
//      .mkString(" "))
//
//    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = session.sparkContext.newAPIHadoopRDD(
//      scanConf,
//      classOf[TableInputFormat],
//      classOf[ImmutableBytesWritable],
//      classOf[Result])
//
//    val rowRDD = hbaseRDD.map(row => {
//      val values: ListBuffer[String] = new ListBuffer[String]
//      val result: Result             = row._2
//
//      for (i <- fields.indices) {
//        if (fields(i).equalsIgnoreCase("rowkey")) {
//          values += Bytes.toString(result.getRow)
//        } else {
//          values += Bytes.toString(result.getValue(Bytes.toBytes(cf), Bytes.toBytes(fields(i))))
//        }
//      }
//      Row.fromSeq(values.toList)
//    })
//    val schema = StructType(
//      fields.map(field => DataTypes.createStructField(field, DataTypes.StringType, true)))
//    val dataFrame = session.createDataFrame(rowRDD, schema)
//    dataFrame
//  }
//}