package org.example.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TransformOperator {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("CreateRDD").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    // TODO 1. RDD的创建--从内存中
    println("=====1. RDD的创建--从内存中=====")
    val seq: Seq[Int] = Seq[Int](1, 2, 3, 4)
    val rdd1: RDD[Int] = sc.parallelize(seq)  // 并行构建方式
    val rdd2: RDD[Int] = sc.makeRDD(seq)  // 一般构建方式，底层实现调用RDD对象的parallelize方法
//    rdd1.collect().foreach(println)
//    rdd2.collect().foreach(println)

    // TODO 2. RDD的创建--从文件中
    println("=====2. RDD的创建--从文件中=====")
    val rdd3: RDD[String] = sc.textFile("data") // 默认以当前环境的相对路径，就是当前项目的根目录，可以是文件也可以是目录
    val rdd4: RDD[String] = sc.textFile("hdfs://node01:9000/user/root/data")  // 读取hdfs文件
    val wholeFile: RDD[(String, String)] = sc.wholeTextFiles("data")  // 返回文件的绝对路径以及内容组成的元组
//    rdd3.collect().foreach(println)
//    rdd4.collect().foreach(println)

    // TODO 3. RDD的并行度（partitions）
    println("=====3. RDD的并行度（partitions）=====")
//    scheduler.conf.getInt("spark.default.parallelism", totalCores)  // 源码中默认的defaultParallelism，当前最大可用核数
    val rdd5: RDD[Int] = sc.makeRDD(seq, 2)  // 指定分区数量，不指定会传入指定并行度defaultParallelism
    sc.textFile("data", 3)  // 看源码得知读取文件的默认分区为2
//    rdd5.saveAsTextFile("output/res01")  // 返回结果中有两个分区文件part-00000、part-00001

    // TODO 4. 算子--map（性能一般）
    println("=====4. 算子--map（性能一般）=====")
    val rdd6 = sc.textFile("data/nginx.log")
    // RDD内计算是一个分区内的一个数据是一个计算逻辑，只有所有的逻辑处理完才会处理下一个分区内的数据
    // 若设置一个分区就是串行处理，若设置多个分区就是并行处理，但是在分区内数据的执行上是串行处理的，所以性能有所欠缺
    val trans: RDD[String] = rdd6.map(_.split(" ")(5))
//    trans.collect().foreach(println)

    // TODO 5. 算子--mapPartitions（强化版map）
    println("=====5. 算子--maPartitions（强化版map）=====")
    // 将一个分区内的数据全部拿到以后才做处理，而不是来一个处理一个，相当于缓冲区
    // 不过一个分区内的数据接收过来以后缓冲为迭代器，需要使用map对迭代器中的每个数据进行处理
    // 但是它会将整个分区数据缓存在内存中，因为对象引用，所以内存较小数据量较大的情况下可能会发生内存溢出，这时就得用map了
    val trans01 = rdd6.mapPartitions(ip => ip.map(_.split(" ")(0)))
//    trans01.collect().foreach(println)

    // TODO 6. 算子--maPartitionsWithIndex（带分区号索引的map）
    println("=====6. 算子--maPartitionsWithIndex（带分区号索引的map）=====")
    val trans02 = rdd6.mapPartitionsWithIndex((idx, ip) => {  // 跟mapPartitions计算方式差不多，只不多多了分区索引index
      if (idx != 1) Nil.iterator
      else ip.map(_.split(" ")(0))  // 只处理分区编号为1的数据
    })
//    trans02.collect().foreach(println)

    // TODO 7. 算子--flatMap（扁平化map）
    println("=====7. 算子--flatMap（扁平化map）=====")
    val rdd7: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4)))
    val trans03: RDD[Int] = rdd7.flatMap(num => num)
//    trans03.collect().foreach(println)

    // TODO 8. 算子--flatMap（扁平化map + 模式匹配）
    println("=====8. 算子--flatMap（扁平化map + 模式匹配）=====")
    val rdd8: RDD[Any] = sc.makeRDD(List(List(1, 2), 5, List(3, 4)))  // 出现了不同的数据类型
    val trans04: RDD[Any] = rdd8.flatMap {  // 在flatMap中使用模式匹配处理不同的数据类型
      case list: List[_] => list
      case data => List(data)
      case _ => List()
    }
//    trans04.collect().foreach(println)

    // TODO 9. 算子--glom（将每个分区中的数据转换为数据，分区不变）
    println("=====9. 算子--glom（将每个分区中的数据转换为数据，分区不变）=====")
    val rdd9: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4), 2)
    val glomRDD: RDD[Array[Int]] = rdd9.glom()  // 将每个分区的数据整合为Array
    val maxRDD: RDD[Int] = glomRDD.map(_.max)  // 使用Array中的max方法求得每个分区的最大值
//    println(maxRDD.collect().sum)  // 再每个分区的最大值进行求和

    // TODO 10. 算子--groupBy（将相同key的值放在一起）
    println("=====10. 算子--groupBy（将相同key的值放在一起）=====")
    val rdd10: RDD[String] = sc.makeRDD(List("Hadoop", "Hello", "Scala", "Python", "PHP"))
    val res01: RDD[(Char, Iterable[String])] = rdd10.groupBy(_.charAt(0))  // 分组和分区没有必然的关系
//    res01.collect().foreach(println)

    // 练习：将nginx.log中的记录按（日期，小时）分组
    val rdd11: RDD[String] = sc.textFile("data/nginx.log").map(_.split("\\[")(1).split("\\]")(0))
    val group: RDD[((String, Int), Iterable[String])] = rdd11.groupBy {
      line => (line.split("/")(0), line.split(":")(1).toInt)
    }  // 对记录进行分割取出日期和小时
//    group.collect().foreach(println)

    // TODO 11. 算子--filter（传入条件返回Boolean）
    println("=====11. 算子--filter（传入条件返回Boolean）=====")
//    rdd9.filter(_ % 2 == 1).collect().foreach(println)  // 筛选奇数
    // 但是筛选过滤后分区不变，分区内的数据可能不平衡，出现数据倾斜

    // TODO 12. 算子--sample（从数据源中抽取一部分数据，考虑抽取完是否放回）
    println("=====12. 算子--sample（从数据源中抽取一部分数据，考虑抽取完是否放回）=====")
    val rdd12: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    // 可以在数据倾斜的时候使用，判断什么原因导致的数据倾斜
//    rdd12.sample(
//      true,  // 抽取后是否放回，true为放回
//      3, // 每条数据被抽取的概率（不放回）;每个元素被期望抽取的次数（放回）
//      2  // 随机数种子，种子相同抽取结果相同
//    ).collect().foreach(println)

    // TODO 13. 算子--distinct（数据去重）
    println("=====13. 算子--distinct（数据去重）=====")
    val rdd13: RDD[Int] = sc.makeRDD(List(1, 1, 3, 3, 5, 5, 3, 8, 4, 4))
    // 底层使用map(x => (x, null)).reduceByKey((x, _) => x, numPartitions).map(_._1)，就是map后reduceByKey再取key值
//    rdd13.distinct().collect().foreach(println)

    // TODO 14. 算子--coalesce（缩减分区）
    println("=====14. 算子--coalesce（缩减分区）=====")
    val rdd14: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7), 3)
//    rdd14.coalesce(2).saveAsTextFile("output/res02")  // 直接进行分区缩减，但可能会出现数据倾斜
//    rdd14.coalesce(2, true).saveAsTextFile("output/res03")  // 加入shuffle阶段，同时使数据变的均衡

    // TODO 15. 算子--repartition（扩大&缩减分区）
    println("=====15. 算子--repartition（扩大&缩减分区）=====")
    // coalesce扩大分区的时候必须使用shuffle阶段，因为不使用shuffle就不会分割分区打乱数据，就达不到扩大分区的目的
    // repartition默认采用shuffle，底层调用的就是coalesce，所以既可以缩减也可以扩大分区
//    rdd14.repartition(4).saveAsTextFile("output/res04")

    // TODO 16. 算子--sortBy（排序）
    println("=====16. 算子--sortBy（排序）=====")
    val rdd15: RDD[Int] = sc.makeRDD(List(4, 5, 6, 3, 2, 1))
    // sortBy会按分区先进行分区内排序，不会减少分区数量
//    rdd15.sortBy(num => num).collect().foreach(println)
    val rdd16: RDD[(String, Int)] = sc.makeRDD(List(("Bob", 1), ("Alice", 10), ("Cli", 3)))
    // 根据元组内的第一个元素排序，默认升序，第二个参数false表示降序
//    rdd16.sortBy(tup => tup._1, false).collect().foreach(println)

    // TODO 17. 算子--intersection、union、subtract、zip（交集、并集、差集、zip）
    println("=====17. 算子--intersection、union、subtract、zip（交集、并集、差集、zip）=====")
    val rdd17: RDD[Int] = sc.makeRDD(List(1, 2, 3))
    val rdd18: RDD[Int] = sc.makeRDD(List(2, 3, 4))
//    println("交集：" + rdd17.intersection(rdd18).collect().mkString("Array(", ", ", ")"))  // 求交集
//    println("并集：" + rdd17.union(rdd18).collect().mkString("Array(", ", ", ")"))  // 求并集
//    println("差集：" + rdd17.subtract(rdd18).collect().mkString("Array(", ", ", ")"))  // 求差集
//    println("键值对：" + rdd17.zip(rdd18).collect().mkString("Array(", ", ", ")"))  // 形成元组，分区数量必须相等，分区中数据的数量也必须相等

    // TODO 18. 算子--partitoinBy（重新分区）
    println("=====18. 算子--partitoinBy（重新分区）=====")
    val rdd19: RDD[(Int, Int)] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3).map((_, 1))
    // 只接受元组类型的RDD，根据指定的分区规则对数据进行重分区，传入一个Partitioner，指定数量，就是一个Hash取模的过程
    // 如果进行两次partitionBy参数相同那么第二次不会进行任何操作，还有一个RangePartitioner用在排序当中使用
//    rdd19.partitionBy(new HashPartitioner(2)).saveAsTextFile("output/res05")

    // TODO 19. 算子--reduceByKey（按键reduce）
    println("=====19. 算子--reduceByKey（按键reduce）=====")
    val rdd20: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 3), ("a", 4), ("b", 2)))
    // 对key相同的值进行两两计算，所以传入两个参数，注意只有一个key时该键值不进行reduceByKey
//    rdd20.reduceByKey(_ + _).collect().foreach(println)

    // TODO 20. 算子--groupByKey（按键group）
    println("=====20. 算子--groupByKey（按键group）=====")
    val res05: RDD[(String, Iterable[Int])] = rdd20.groupByKey()  // 将相同key对应的value放一起
    val res06: RDD[(String, Iterable[(String, Int)])] = rdd20.groupBy(_._1)  // 注意两者不同groupByKey将value取出来了
//    res05.collect().foreach(println)
//    res06.collect().foreach(println)

    // TODO 21. 算子--aggregateByKey（区分分区内聚合和分区间聚合）
    println("=====21. 算子--aggregateByKey（区分分区内聚合和分区间聚合）=====")
    val rdd21: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 3), ("a", 4), ("a", 2)), 2)
    // 使用的是柯里化方式，第一个参数列表传入初始值，因为分区内要做两两计算，第一个分区内的值要和分区值做计算，
    // 第二个参数列表传入两个参数，第一个参数是分区内聚合，第二个参数是分区间聚合
//    rdd21.aggregateByKey(0)(
//      (x, y) => math.max(x, y),
//      (x, y) => x + y
//    ).collect().foreach(println)

    // TODO 22. 算子--foldByKey（分区内聚合和分区间聚合相同）
    println("=====22. 算子--foldByKey（分区内聚合和分区间聚合相同）=====")
//    rdd21.foldByKey(10)(_ + _).collect().foreach(println)  // 其实就是reduceByKey有初始值

    // TODO 23. 算子--join（内连接）
    println("=====23. 算子--join（内连接）=====")
    val rdd22: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("e", 3)))
    val rdd23: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("b", 5), ("d", 6)))
//    println("join" + rdd22.join(rdd23).collect().mkString("Array(", ", ", ")"))
//    println("fullOuterJoin" + rdd22.fullOuterJoin(rdd23).collect().mkString("Array(", ", ", ")"))
//    println("leftOuterJoin" + rdd22.leftOuterJoin(rdd23).collect().mkString("Array(", ", ", ")"))
//    println("rightOuterJoin" + rdd22.rightOuterJoin(rdd23).collect().mkString("Array(", ", ", ")"))

    // TODO 24. 算子--cogroup（分组 + 连接）
    println("=====23. 算子--cogroup（分组 + 连接）=====")
    // 它是先分组再连接，把同一RDD下相同key的值先放一起然后再连接其他RDD，返回两个迭代器组成的元组，最多支持三个参数
    val rdd24: RDD[(String, Int)] = sc.makeRDD(List(("a", 4), ("a", 5), ("d", 6)))
//    val res07: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd22.cogroup(rdd24)
//    res07.collect().foreach(println)

    // TODO 25. 案例练习
    println("=====25. 案例练习=====")
    // 数据是时间戳 省份 市 用户 广告五个字段，计算每个省份点击次数前三名的广告
    val original: RDD[String] = sc.textFile("data/agent.log")  // 时间戳 省份 市 用户 广告
    val mapRDD: RDD[((String, String), Int)] = original.map(str => {
      val strings: Array[String] = str.split(" ")  // 拆分数据并取到省份和广告字段
      ((strings(1), strings(4)), 1)
    })
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)  // 计算每个省份每条广告的点击人数
    // 转换数据结构，因为最终是计算每个省份内的，所以省份是key，将广告跟点击数放一起
    val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map{ case ((pro, ad), sum) => (pro, (ad, sum)) }
    // groupBy省份，将所有省份下的所有广告点击数放一起
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()
    // 在每个省份内排序，取前三条数据
    val mapValuesRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(iter => {
      iter.toList.sortWith(_._2 > _._2).take(3) // sortBy(_._2)(Ordering.Int.reverse)
    })
    mapValuesRDD.collect().foreach(println)










    sc.stop()

  }
}
