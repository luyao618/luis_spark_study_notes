# 问题
> spark是并行处理的大数据框架。所以，很多时候程序的运行速度，失败原因都和并行度有关。那什么是并行度？并行度怎么设置？
# 并行度的概念
## 资源并行度与数据并行度
> 我理解的有两类并行度，一种时资源的并行度，由节点数(executor)和cpu数(core)决定的。另一种并行度就是task的数据，也就是partition大小。task又分为map时的task和reduce(shuffle)时的task.task的数目和很多因素有关，资源的总core数，spark.default.parallelism参数，spark.sql.shuffle.partitions参数，读取数据源的类型,shuffle方法的第二个参数,repartition的数目等等。 
## 这两种并行度的关系
> 如果task的数量多，能用的资源也多，那么并行度自然就好。如果task的数据少，资源很多，有一定的浪费，但是也还好。如果task数目很多，但是资源少，那么会执行完一批，再执行下一批。所以官方给出的建议是，这个task数目要是core总数的2-3倍为佳。如果core有多少task就有多少，那么有些比较快的task执行完了，一些资源就会处于等待的状态。
## 关于数据并行度
### 初始化RDD时
> 数据化RDD时，所形成的task数据是和HDFS里的Block有关，之前我以为一个Block会是一个task，后来发现并不是这样，task的数目是和InputSplit一一对应的，InputSplit是由若干个Block合成的。读数据时也是可以设置,textFile的第二个参数可以指定。如果既有参数也有InputSplit，会取两者较大的。
### Map操作时
> 在Map类算子这种窄依赖操作时，partition的数目不变和父RDD相同。
### Reducer操作时
> 对于reduce操作，假设时*ByKey，其Partition数量依如下顺序确定：1. 方法的第二个参数 > 2. spark.default.parallelism参数 > 3. 所有依赖的RDD中，Partition最多的RDD的Partition的数量。

## 并行度调优
- 最简单粗暴的就是repartition算子，如果知道哪个步骤需要调整并行度，那么在这个步骤执行之前，调用repartition(${partitionNum})就可以了。有一定的shuffle消耗，但是有些情况下会提升程序的执行速度很多很多，特别是碎片化文件特别严重的时候有奇效。但是有个局限，这个输入的partition是写死的，不够灵活。
- reduce算子指定partition，这个和repartition类似。
-  spark.defalut.parallelism   默认是没有值的，如果设置了值比如说10，是在shuffle的过程才会起作用（val rdd2 = rdd1.reduceByKey(_+_) //rdd2的分区数就是10，rdd1的分区数不受这个参数的影响）
-  spark.sql.shuffle.partitions //spark sql中shuffle过程中partitions的数量
-  val rdd3 = rdd1.join（rdd2）  rdd3里面partiiton的数量是由父RDD中最多的partition数量来决定，因此使用join算子的时候，增加父RDD中partition的数量 
-  *（关于spark.sql.shuffle.partitions和spark.defalut.parallelism 这两个参数配置。一个是在spark sql文档里的，描述上说（Configures the number of partitions to use when shuffling data for joins or aggregations.
）默认200。另一个是spark文档里的参数(Default number of partitions in RDDs returned by transformations like join, reduceByKey, and parallelize when not set by user.)。我所理解的是当使用当spark sql且两者都有设置，shuffle.partitions会起作用，如果不是spark sql的shuffle，需要defalut.parallelism才起作用）*

# 总结
> 关于spark并行度及其调整优化的理解就是这些，能力尚浅，理解有误欢迎指正。对比实际的情况，特别是及其复杂的程序逻辑活着pyspark，有些stage的task划分还是有些疑惑的。