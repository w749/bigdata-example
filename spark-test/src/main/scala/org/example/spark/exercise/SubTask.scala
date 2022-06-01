package org.example.spark.exercise

class SubTask extends Serializable {
  // 子任务类，用以处理数据类中的计算和逻辑并计算返回结果
  var data: List[Int] = _
  var calculate: Int => Int = _
  def func(): List[Int] = data.map(calculate)
}
