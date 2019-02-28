# 问题
> 对于spark streaming程序和spark程序的区别，最大的一个问题就是spark streaming程序需要控制每次处理的时间。我们看以下两种场景，都很常见。

## 场景1:
**程序每次处理的数据量是波动的，比如周末比工作日多很多，晚八点比凌晨四点多很多。**  
一个spark程序处理的时间在1-2小时波动是OK的。而spark streaming程序不可以,如果每次处理的时间是1-10分钟，就很蛋疼。  
设置10分钟吧，实际上10分钟的也就那一段高峰时间，如果设置每次是1分钟，很多时候会出现程序处理不过来，排队过多的任务延迟更久，还可能出现程序崩溃的可能。
## 场景2:
**程序需要处理的相似job数随着业务的增长越来越多**  
我们知道spark的api里无相互依赖的stage是并行处理的，但是job之间是串行处理的。  
spark程序通常是离线处理，比如T+1之类的延迟，时间变长是可以容忍的。而spark streaming是准实时的，如果业务增长导致延迟增加就很不合理。

## 举个例子：
目前在做的反作弊规则引擎，离线的处理程序是spark，在线的规则引擎是spark streaming。   
离线的问题不大，在线规则引擎如果因为规则越来越多，每次batch处理的时间越来越长是不合理的。

# 解决方案
spark虽然是串行执行job，但是是可以把job放到线程池里多线程执行的。[如何在一个SparkContext中提交多个任务](https://qindongliang.iteye.com/blog/2382800)
  
而spark streaming同样可以。  
**以下是关键代码：**
```
DStream.foreachRDD{
      rdd =>
        //创建线程池
        val executors=Executors.newFixedThreadPool(rules.length)
        //将规则放入线程池
        for( ru <- rules){
          val task= executors.submit(new Callable[String] {
            override def call(): String ={
              //执行规则
              runRule(ru,spark)
            }
          })
        }
        //每次创建的线程池执行完所有规则后shutdown
        executors.shutdown()
    }
```
## 注意点
1.**最后需要executors.shutdown()**。  
如果是executors.shutdownNow()会发生未执行完的task强制关闭线程。  
如果使用executors.awaitTermination()则会发生阻塞，不是我们想要的结果。  
如果没有这个shutdowm操作，程序会正常执行，但是长时间会产生大量无用的线程池，因为每次foreachRDD都会创建一个线程池。    

2.**可不可以将创建线程池放到foreachRDD外面？**  
**不可以**，这个关系到对于scala闭包到理解，经测试，第一次或者前几次batch是正常的，后面的batch无线程可用。  

3.**线程池executor崩溃了就会导致数据丢失**  
原则上是这样的，但是正常的代码一般不会发生executor崩溃。至少我在使用的时候没遇到过。
 
# 结果
对于目前的规则引擎项目，使用了多线程并发提交Job的操作，可以在**毫秒级处理多数据源大吞吐量且存在数据量波动的数据流**。并且在资源充足的情况下，可以随意横向拓展业务且不增加延迟时间。