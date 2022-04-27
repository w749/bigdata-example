package org.mlamp.hbase


object MyCell {
  private var myCell: MyCell = _
  def builder(rowKey: Array[Byte], family: Array[Byte], qualifier: Array[Byte], value: Array[Byte], timestamp: Long): MyCell = {
    this.myCell = MyCell(rowKey, family, qualifier, value, timestamp)
    this.myCell
  }
}
case class MyCell(rowKey: Array[Byte], family: Array[Byte], qualifier: Array[Byte], value: Array[Byte], timestamp: Long) {
  override def toString: String = {
    s"{RowKey: ${new String(rowKey)}, " +
      s"Column Family: ${new String(family)}, " +
      s"Qualifier: ${new String(qualifier)}, " +
      s"Value: ${new String(value)}, " +
      s"Timestamp: ${timestamp}}"
  }
}

