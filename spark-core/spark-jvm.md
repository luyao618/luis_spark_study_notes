# 问题
> 一个spark streaming程序，在运行一段时间后会突然遇到批次处理暴增，然后大于batch时间，导致程序挂调甚至假死（sc stop但是ssc正常运行）查看日志看到gc OOM的错误，其他没有发现明显问题。（PS：程序数据量不大，资源够用。程序中有用到scala反射函数、调用java实现的文本模型）于是学习了下jvm的调优，希望能解决executor gc的问题。

# JVM调优
## 不进行JVM调优
> 如果能够通过资源调优和内存调优解决问题，最好不要进行JVM调优。因为jvm的调优是非常复杂和敏感的，而绝大多数人应该都像我一样对此一知半解的，所以尽量不要瞎调。


> [spark资源调优（留坑）]() 主要是加executor内存的大小

> [spark内存调优（留坑）]() 主要是调整spark.storage.memoryFraction参数的大小，默认是0.6如果Cache操作比较少，可以把这个比例调小一点，多给task运行一些内存。
其次就是堆外内存，例如：spark.yarn.executor.memoryOverhead=2048

## 我偏要调JVM参数
### 简单了解下理论基础
- spark是用scala开发的。   
spark的scala代码调用了很多java api。    
scala也是运行在java虚拟机中的。spark是运行在java虚拟机中的。

- 我们的RDD的缓存、task运行定义的算子函数，可能会创建很多对象。  都可能会占用大量内存，没搞好的话，可能导致JVM出问题。
- 当一个eden，survivor 区域放满之后，触发minor gc 小型垃圾回收。eden：survivor：survivor = 8:1:1 如果一次gc后，存活下来的对象需要占用1.5 个survivor。 
那么一个survivor 放不下。 
就会根据担保机制，把多余的对象，直接放入老年代了。
- 如果你的JVM内存不够大，可能导致年轻代频繁地内存满溢，频繁进行minor gc。
- 频繁的触发minor gc，这样会导致一些短生命周期的对象进入到老年代，老年代的对象不断的囤积，最终触发full gc。  
- 一次full gc会使得所有其他程序暂停很长时间。最终严重影响我们的Spark的性能和运行速度。
- full gc/minor gc 无论是快还是慢， 都会导致jvm的 工作线程停止工作。

### 默认JVM参数

>Executor的JVM参数：
-Xmx，-Xms，如果是yarn-client模式，则默认读取spark-env文件中的SPARK_EXECUTOR_MEMORY值，-Xmx，-Xms值一样大小；如果是yarn-cluster模式，则读取的是spark-default.conf文件中的spark.executor.extraJavaOptions对应的JVM参数值。
PermSize，两种模式都是读取的是spark-default.conf文件中的spark.executor.extraJavaOptions对应的JVM参数值。
GC方式，两种模式都是读取的是spark-default.conf文件中的spark.executor.extraJavaOptions对应的JVM参数值。  

> 网上别人说的，我也不确定，反正没事也不看。

### 开始作死
> 首先了解下有哪些参数可以调：[JVM参数大全](https://blog.csdn.net/kthq/article/details/8618052)

> spark jvm调整中可能经常调的参数：  
```
    //-XX:+CMSClassUnloadingEnabled 当首次遭遇内存溢出时Dump出此时的堆内存。
    //-XX:+UseConcMarkSweepGC Use CMS收集 较高cpu使用率 减少full gc时间
    //-XX:+UseCMSCompactAtFullCollection：打开内存空间的压缩和整理，在Full GC后执行。可能会影响性能，但可以消除内存碎片。
    //-XX:CMSInitiatingOccupancyFraction=70：表示年老代内存空间使用到70%时就开始执行CMS收集
    //-XX:-HeapDumpOnOutOfMemoryError：当首次遭遇内存溢出时Dump出此时的堆内存。
    SparkConf.set("spark.executor.extraJavaOptions","-XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC -XX:+UseCMSCompactAtFullCollection -XX:CMSInitiatingOccupancyFraction=70 -XX:+HeapDumpOnOutOfMemoryError")

```

# 最后
上文说到的问题程序已经经过了一系列调整，在灰度跑着，默默等待中。
