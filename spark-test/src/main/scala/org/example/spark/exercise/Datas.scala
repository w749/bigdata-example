package org.example.spark.exercise

class Datas extends Serializable {
  // 数据类，存储所有的数据和计算逻辑
  val data = List(1, 3, 5, 7)
  val calculate: Int => Int = _ * 2
}
