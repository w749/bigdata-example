package org.example.curator

import java.util.Properties

object Config {
  def apply(path: String): Properties = {
    val properties = new Properties()
    try {
      properties.load(this.getClass.getClassLoader.getResourceAsStream(path))
    } catch {
      case e: Exception => println("获取 Properties 对象失败" + e)
    }
    properties
  }
}

class Config