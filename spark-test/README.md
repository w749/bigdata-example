# Spark应用案例

## 工具函数
[Utils](https://github.com/w749/bigdata-example/blob/master/spark-test/src/main/scala/org/example/spark/Utils.scala)

常用工具函数

## Spark正则多模匹配
[HyperScanJava](https://github.com/w749/bigdata-example/blob/master/spark-test/src/main/scala/org/example/spark/cases/HyperScanJava.scala)

使用 [hyperscan-java](https://github.com/gliwka/hyperscan-java) 完成多个正则表达式快速匹配一个字符串的需求，
它其实是对C语言编写的HyperScan做了封装，使得效率大大提升，具体查看 [hyperscan](https://github.com/intel/hyperscan) 

## Spark解压缩tar.gz
[TarGzipCodec](https://github.com/w749/bigdata-example/blob/master/spark-test/src/main/scala/org/example/spark/cases/TarGzipCodec.scala)

自定义CompressionCodec实现输入输出tar.gz压缩格式。其中读取数据的实现没什么问题，压缩为tar.gz的时候会有一些问题，
创建tar输出流时必须指定一个TarArchiveEntry的size，代表需要归档的数据大小，但是spark运行时无法获取最终写入的数据大小，所以就无法通过这种方式写入，
最终想到一个办法是先将需要写入的数据放在一个ArrayBuffer中，close流的时候再遍历写入，这种方法在输出数据量特别大时可能会造成内存溢出

[compressTextFile](https://github.com/w749/bigdata-example/blob/master/spark-test/src/main/scala/org/example/spark/cases/TarGzipCodec.scala#L48)

还可以通过另外一种方式就是先输出为text file，然后再将它压缩为tar.gz文件，缺点是先要写出到一个临时目录中，而且text file会占用大量空间，不过好在这种方式速度并不会很慢

## Spark多目录输出
[MultipleOutputFormat](https://github.com/w749/bigdata-example/blob/master/spark-test/src/main/scala/org/example/spark/cases/MultipleOutputFormat.scala)

自定义MultipleTextOutputFormat，满足根据key自定义输出目录以及输出文件名称的需求，并且不输出key

## Spark获取输入的数据所属文件名称
[GetInputFileName](https://github.com/w749/bigdata-example/blob/master/spark-test/src/main/scala/org/example/spark/cases/GetInputFileName.scala)

Spark获取输入的数据所属文件名称，如需获取全路径可以在getPath后调用toUri方法再调用getName

## Spark指定每个分区的输出大小
[ControlPartitionSize](https://github.com/w749/bigdata-example/blob/master/spark-test/src/main/scala/org/example/spark/cases/ControlPartitionSize.scala)

Spark指定每个分区的输出大小，提供了三种方法，分别是自定义分区器（repartitionData）、重分区（restoreData），这两种方法主要利用了FileSystem的getContentSummary方法获取到输入数据的大小，计算出输出指定大小的分区所需的分区数量，
第三种方法通过控制split size的目的达到控制每个分区的数据大小。这几种方式必须是输入和输出数据相同未经过过滤或者flat，如果输入输出数据大小不相同可以借助临时目录
