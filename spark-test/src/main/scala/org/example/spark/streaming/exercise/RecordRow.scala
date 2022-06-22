package org.example.spark.streaming.exercise

case class RecordRow(id: Int, name: String = "", timestamp: Long) {
  override def toString: String = {
    id + "," + name + "," + timestamp
  }
}
