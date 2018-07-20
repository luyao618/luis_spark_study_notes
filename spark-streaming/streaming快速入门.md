# 1.Overview
>spark streaming是spark的一个拓展应用，对实时数据流进行：可拓展、高吞吐、 可容错的流处理。
数据可以从多个来源得到，比如：Kafka，Flume，Kinesis或者TCP socket，并提供高级别的函数诸如map,reduce,join和window这样复合的算法。最终处理后的数据可以通过文件系统、数据库和实时dashboards输出。还支持Spark的机器学习和图形处理算法在数据流上。

![streaming-arch.png](https://spark.apache.org/docs/latest/img/streaming-arch.png)
>内部是按照如下方式处理的。Spark Streaming接收实时输入数据并将数据分成多个batches，然后Spark engine会产生最终的结果batches流。

![streaming-flow.png](https://spark.apache.org/docs/latest/img/streaming-flow.png)

>如果做个比喻，如果把数据流比喻成人流，那么spark streaming就像地铁一样运输（处理）人流（数据流）。我们设置的batch time就像地铁班次之间的间隔。我们设置的executor就像设置的地铁车厢数。人流不断，地铁不停。上下班高峰时人流量过大，地铁运输不过来，也可能产生排队(延迟)。如果每次可以处理的数据量（每辆地铁可运输最大人数）大于batch time时间内产生的数据量，那么程序是健康状态，没有延迟（排队）

![地铁](http://upload.qianlong.com/2017/0910/1505029992266.jpg)

# 2.Qcick Example
## 2.1添加依赖

```
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion % "provided"
```
## 2.2初始化
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

val conf = new SparkConf()
val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
val sc = spark.sparkContext
val ssc = new StreamingContext(sc, Seconds(batchDuration))

```
## 2.2创建DStream
>通过消费kafka创建DStream
```scala
import org.apache.spark.streaming.kafka.KafkaUtils
kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)
//type: InputDStream[(String, String)]
DStream.map{record => (record._1,record._2) }.print()
//record的_1是key,_2是value，我们一般接入kafka数据是获取的value
```
## 2.3执行
>注意，执行这些行时，Spark Streaming仅设置启动时将执行的计算，并且尚未启动实际处理。要在设置完所有转换后开始处理，我们最终调用
```scala
ssc.start()             // 开始计算
ssc.awaitTermination()  // 等待计算终止
```

# 3.深入理解
## 3.1 DStream（Discretized Stream 离散 流）
>Spark Streaming提供一个高级别的抽象概念：discretized stream（DStream），来代表一个连续的数据流。DStream既可以从Kafka、Flume和Kinesis输入数据流中创建，也可以从其他DStream上通过高级别的操作产生。在内部DStream是由一连串的RDDs来表示。
Dstream 没有边界 没有大小，随着时间的推移，不断产生RDD。可以理解为RDD又加了个时间维度。

![image1](https://spark.apache.org/docs/latest/img/streaming-dstream.png)
>应用于DStream的任何操作都转换为底层RDD上的操作。我们熟悉的word count中flatMap操作应用于DStream行中的每个RDD，以生成单词DStream的RDD。如下图所示。

![image2](https://spark.apache.org/docs/latest/img/streaming-dstream-ops.png)
>这些底层RDD转换由Spark引擎计算。 DStream操作隐藏了大部分细节，并为开发人员提供了更高级别的API以方便使用。
与RDD类似，转换允许修改来自输入DStream的数据。 DStreams支持普通Spark RDD上可用的许多转换。一些常见的如下。


Transformation | Meaning
-- | --
map(func) | Return a new DStream by passing each element of the source DStream through a function func.
flatMap(func) |	Similar to map, but each input item can be mapped to 0 or more output items.
filter(func) |	Return a new DStream by selecting only the records of the source DStream on which func returns true.
repartition(numPartitions) |	Changes the level of parallelism in this DStream by creating more or fewer partitions.
union(otherStream) |	Return a new DStream that contains the union of the elements in the source DStream and otherDStream.
count()	 | Return a new DStream of single-element RDDs by counting the number of elements in each RDD of the source DStream.

## 3.2 窗口操作
>Spark Streaming还提供窗口计算，允许滑动数据窗口上应用转换。窗口操作需要指定俩个参数窗口长度（window length）和划定距离（sliding interval）这两个参数都必须是batch time的整数倍。

![image](https://spark.apache.org/docs/latest/img/streaming-dstream-window.png)

```scala
DStream.window(Seconds(windowLength) ,Seconds(slideLength)).foreachRDD{
    ...
}
//window函数 返回一个新的DStream，它是根据源DStream的窗口批次计算的
```
## 3.3 foreachRDD
>dstream.foreachRDD是最常用的output operations 函数（类比RDD里的action操作），这个函数很强大使用这个函数可以让熟练spark的用户快速掌握spark streaming编程。
值得注意的一点是，一定要清楚foreachRDD那些操作是在driver端执行，哪些操作是在worker上执行
```scala
dstream.foreachRDD { rdd =>
  val connection = createNewConnection()  // executed at the driver
  rdd.foreach { record =>
    connection.send(record) // executed at the worker
  }
}
```
> 使用 foreachRDD 结合spark SQL

```scala
dstream.foreachRDD { rdd =>

  // 这个写在这里和写在foreachRDD外是一样的效果
  val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
  import spark.implicits._

  // 对rdd进行操作
  val wordsDataFrame = rdd.map{
    ...
  }.toDF("word")

  // 创建内存表
  wordsDataFrame.createOrReplaceTempView("words")

  // spark sql操作
  val wordCountsDataFrame =
    spark.sql("select word, count(*) as total from words group by word")
  wordCountsDataFrame.save(...)
}
```
