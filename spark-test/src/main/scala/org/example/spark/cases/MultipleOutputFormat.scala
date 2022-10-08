package org.example.spark.cases

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.mapred.{JobConf, RecordWriter, Reporter}
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.util.Progressable
import org.apache.spark.{SparkConf, SparkContext}
import org.example.spark.Utils

import java.text.SimpleDateFormat
import java.util
import java.util.Date

/**
 * 自定义MultipleTextOutputFormat，满足根据key自定义输出目录以及输出文件名称的需求，并且不输出key
 */
object MultipleOutputFormat {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("Whitelist")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(sparkConf)
    val output = "data/test-output/tmp"
    Utils.cleanHadoopPath(sc, output)

    sc.makeRDD(Array(("get", "dWE=||1662443766000"), ("get", "dWE=||1662443766000"), ("post", "dWE=||1662443766000"), ("post", "dWE=||1662443766000")))
      .saveAsHadoopFile(
        output,
        classOf[String],
        classOf[String],
        classOf[MultipleOutputFormat]
      )
  }
}


/**
 * 根据数据内容自定义输出目录和文件名
 */
class MultipleOutputFormat extends MultipleTextOutputFormat[String, String] {
  override def generateFileNameForKeyValue(key: String, value: String, name: String): String = {
    val split = value.split("\\|")
    val ts = split(2).toLong * 1000
    val date = new SimpleDateFormat("yyyyMMdd").format(new Date(ts))
    val hour = new Date(ts).getHours.toString
    val newName = s"part-${key}-${date}-${hour}-${name.split("-")(1)}"
    if (key == "get") s"get/${newName}" else s"post/${newName}"
  }

  // 自定义RecordWriter，不输出key
  override def getRecordWriter(fs: FileSystem, job: JobConf, name: String, arg3: Progressable): RecordWriter[String, String] = {
    val myFS: FileSystem = fs
    val myName: String = generateLeafFileName(name)
    val myJob: JobConf = job
    val myProgressable: Progressable = arg3

    new RecordWriter[String, String]() {
      val recordWriters: util.TreeMap[String, RecordWriter[String, String]] = new util.TreeMap[String, RecordWriter[String, String]]

      override def write(key: String, value: String): Unit = {
        val keyBasedPath: String = generateFileNameForKeyValue(key, value, myName)
        val finalPath: String = getInputFileBasedOutputFileName(myJob, keyBasedPath)
        val actualKey: String = null  // 指定为null即可，它也不会输出制表符，查看org.apache.hadoop.mapred.TextOutputFormat.LineRecordWriter.write方法
        val actualValue: String = generateActualValue(key, value)
        var rw: RecordWriter[String, String] = this.recordWriters.get(finalPath)
        if (rw == null) {
          rw = getBaseRecordWriter(myFS, myJob, finalPath, myProgressable)
          this.recordWriters.put(finalPath, rw)
        }
        rw.write(actualKey, actualValue)
      }

      override def close(reporter: Reporter): Unit = {
        val keys: util.Iterator[String] = this.recordWriters.keySet.iterator
        while ( {
          keys.hasNext
        }) {
          val rw: RecordWriter[String, String] = this.recordWriters.get(keys.next)
          rw.close(reporter)
        }
        this.recordWriters.clear()
      }
    }
  }
}
