package org.mlamp.util

import java.io.File
import java.util.regex.Pattern
import scala.collection.mutable.ListBuffer

class FileUtils {
  /**
   * 新建空白文件
   */
  def createNewFile(path: String, over: Boolean = false): Unit = {
    val file = new File(path)
    if (!file.exists) file.createNewFile() else {
      if (over) {
        file.delete()
        file.createNewFile()
      }
    }
  }

  /**
   * 新建目标文件夹
   * @param path 路径
   * @param over 是否覆盖
   */
  def mkDirs(path: String, over: Boolean = false): Unit = {
    val file = new File(path)
    if (!file.exists) file.mkdirs else {
      if (over) {
        delDirs(path)
        file.mkdirs
      }
    }
  }

  /**
   * 递归删除本地文件夹
   */
  def delDirs(path: String): Unit = {
    val file = new File(path)
    if (file.exists) {
      if (file.isDirectory) {
        val files = file.listFiles()
        val len = files.length
        if (len != 0) {
          files.foreach(ff => if (ff.isFile) ff.delete() else delDirs(ff.getPath))
        }
        file.delete()
      } else {
        file.delete
      }
    }
  }

  /**
   * 匹配目录下符合条件的文件，只遍历当前的文件夹
   */
  def matchFile(path: String, pattern: String): ListBuffer[File] = {
    val file = new File(path)
    val lst = ListBuffer[File]()
    if (file.isDirectory) {
      file.listFiles.foreach { ff =>
        if (ff.isFile && Pattern.matches(pattern, ff.getName)) {
          lst += ff
        }
      }
    } else {
      if (Pattern.matches(pattern, file.getName)) {
        lst += file
      }
    }
    lst
  }
}
