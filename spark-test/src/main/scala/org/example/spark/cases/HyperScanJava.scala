package org.example.spark.cases

import com.gliwka.hyperscan.wrapper.{CompileErrorException, Database, Expression, ExpressionFlag, Scanner}
import org.example.spark.Utils

import java.util

/**
 * 测试HyperScan正则表达式，多模正则表达式匹配，底层使用intel开源是哟个C语言写的hyperscan正则匹配引擎
 *   所需依赖com.gliwka.hyperscan.hyperscan
 *   GitHub地址：https://github.com/gliwka/hyperscan-java
 */
object HyperScanJava {
  def main(args: Array[String]): Unit = {
    val start = System.currentTimeMillis()
    var db: Database = null
    val expressions = new util.LinkedList[Expression]()
    // 这里输入正则表达式文件
//    val source = Source.fromFile("src\\main\\data\\reduce\\ua-regex.csv")
//    source.getLines().foreach(str => expressions.add(new Expression(str, ExpressionFlag.SINGLEMATCH)))
//    source.close()
    // 这里输入单个正则表达式，注意不同的flag对应不同的匹配规则
    expressions.add(new Expression(".*\\.ce\\.cn$", util.EnumSet.of(ExpressionFlag.SINGLEMATCH)))

    // 解析正则表达式，解析错误时捕获
    try {
      db = Database.compile(expressions)
    } catch {
      case e: CompileErrorException => println("Pattern compile error: " + e.getFailedExpression)
    }

    val scanner = new Scanner()
    scanner.allocScratch(db)
    // 这里放入待匹配数据
    val matches = scanner.scan(db, "%25E3%2583%259E%25E3%2583%2583%25E3%2583%2597ttt")

    val iterator = matches.iterator()
    println("是否匹配 " + iterator.hasNext)
    while (iterator.hasNext) {
      val value = iterator.next()
      println("=====")
      println(value.getMatchedString)
      println(value.getMatchedExpression.getExpression)
      println(value.getStartPosition)
      println(value.getEndPosition)
    }
    val end = System.currentTimeMillis()
    println("共耗时：" + Utils.timeInterval(start, end))
  }
}
