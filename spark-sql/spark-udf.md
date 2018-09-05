
>UDF(User-defined functions, UDFs),即用户自定义函数，在Spark Sql的开发中十分常用，UDF对表中的每一行进行函数处理，返回新的值，有些类似与RDD编程中的Map()算子，实际开发中几乎每个Spark程序都会使用的。今天梳理一下相关的知识点，每个分支下都有测试的示例代码。

## 1.scala
>第一部分是scala进行Spark程序开发。
### 1.1 udf
>在spark sql中有两种注册udf的方法，这里做一下对比。  
首先，准备一个DataFrame和一个函数
```scala
import spark.implicits._

    //生成一个DataFrame
    val df = Seq(
      (1, "boy", "裤子"),
      (2, "girl", "裤子"),
      (3, "boy", "裙子"),
      (4, "girl", "裙子"),
      (5, "girl", "裙子")
    ).toDF("id", "sex", "dressing")
    df.createOrReplaceTempView("boys_and_girls")

    //找变态函数
    def findHentai(sex:String,dressing:String): String ={
      if(sex =="boy" && dressing == "裙子") "变态" else "正常"
    }

```
#### 1.1.1.udf()
>调用spark.sql.function.udf()方法
```scala
    //注册函数
    //常用写法
    spark.udf.register("find_hentai",findHentai _ )
    //这种写法也行
    spark.udf.register("findHentai",findHentai(_:String,_:String))
    //这个也是一样的，spark2之前的都是用sqlContext去注册
    spark.sqlContext.udf.register("find_the_one",findHentai _)
    //调用udf
    spark.sql(s"select id,sex,dressing,find_the_one(sex,dressing) as tag from boys_and_girls").show()
```
> 结果如下
```
+---+----+--------+-----+
| id| sex|dressing| tag |
+---+----+--------+-----+
|  1| boy|  裤子  |正常 |
|  2|girl|  裤子  |正常 |
|  3| boy|  裙子  |变态 |
|  4|girl|  裙子  |正常 |
|  5|girl|  裙子  |正常 |
+---+----+--------+-----+
```
#### 1.1.2.register()
>调用 sqlContext.udf.register()方法
```scala
    //需要导入相关的包
    import org.apache.spark.sql.functions.{udf,col}
    //注册函数
    val who_is_hentai = udf(findHentai(_:String,_:String))
    //调用函数
    df.select(col("id"),col("sex"),col("dressing"),who_is_hentai(col("sex"),col("dressing"))).show()
    //结果一样
```
#### 1.1.3.对比
>对与DataFrame我们用两种开发模式，一种是利用sql()，将程序逻辑用sql表示，把数据注册成历史表，进行sql开发，适合sql写的比较6的选手。另外一种就是直接对DataFrame调用API。两种方式我没有具体深入源码比较过，但是直觉还是那个感觉效率应该是一样的。  
所以对于这两种模型，udf也有两种使用模式。    
两种方式代码量上差不多，  
第一种方式相对常见和更容易理解一些。

### 1.2 udaf
>除了逐行处理数据的udf，还有比较常见的就是聚合多行处理udaf，自定义聚合函数。类比rdd编程就是map和reduce算子的区别。  
自定义UDAF，需要extends  org.apache.spark.sql.expressions.UserDefinedAggregateFunction，并实现接口中的8个方法。  
udaf写起来比较麻烦，我下面列一个之前写的取众数聚合函数，在我们通常在聚合统计的时候可能会受某条脏数据的影响。  
举个栗子：  
对于一个app日志聚合的时候，有id与ip，原则上一个id有一个ip，但是在多条数据里有一条ip是错误的或者为空的，这时候group能会聚合成两条数据了就，如果使用max，min对ip也进行聚合，那也不太合理，这时候可以进行投票，去类似多数对结果，从而聚合后只有一个设备。  
废话少说，上代码：  
```scala

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


/**
  * 自定义聚合函数：众数（取列内频率最高的一条）
  * Created by luis on 2017/9/25.
  */
class UDAFGetMode extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = {
    StructType(StructField("inputStr",StringType,true):: Nil)
  }

  override def bufferSchema: StructType = {
    StructType(StructField("bufferMap",MapType(keyType = StringType,valueType = IntegerType),true):: Nil)
  }

  override def dataType: DataType = StringType

  override def deterministic: Boolean = false

  //初始化map
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = scala.collection.immutable.Map[String,Int]()
  }

  //如果包含这个key则value+1，否则 写入key，value=1
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val key = input.getAs[String](0)
    val immap = buffer.getAs[scala.collection.immutable.Map[String,Int]](0)
    val bufferMap = scala.collection.mutable.Map[String,Int](immap.toSeq: _*)
    val ret = if (bufferMap.contains(key)){
      val new_value = bufferMap.get(key).get + 1
      bufferMap.put(key,new_value)
      bufferMap
    }else{
      bufferMap.put(key,1)
      bufferMap
    }
    buffer.update(0,ret)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //合并两个map 相同的key的value累加
    //http://www.cnblogs.com/tugeler/p/5134862.html
    buffer1.update(0,( buffer1.getAs[scala.collection.immutable.Map[String,Int]](0) /: buffer2.getAs[scala.collection.immutable.Map[String,Int]](0) ) { case (map, (k,v)) => map + ( k -> (v + map.getOrElse(k, 0)) ) })
  }

  override def evaluate(buffer: Row): Any = {
    //返回值最大的key
    var max_vale = 0
    var max_key = ""
    buffer.getAs[scala.collection.immutable.Map[String,Int]](0).foreach{
      x=>
        val key = x._1
        val value = x._2
        if(value>max_vale) {
          max_vale=value
          max_key=key
        }
    }
    max_key
  }
}

```
>udaf的使用
```scala
    spark.udf.register("get_mode", new com.meitu.utils.spark.udaf.UDAFGetMode)

    import spark.implicits._

    val df = Seq(
      (1, "10.10.1.1", "start"),
      (1, "10.10.1.1", "search"),
      (2, "123.123.123.1", "search"),
      (1, "10.10.1.0", "stop"),
      (2, "123.123.123.1", "start")
    ).toDF("id", "ip", "action")

    df.createOrReplaceTempView("tb")

    spark.sql(s"select id,get_mode(ip) as u_ip,count(*) as cnt from tb group by id").show()
```

```
+---+--------------+------+
| id|     u_ip     | cnt  |
+---+--------------+------+
|  1| 10.10.1.1    |  3   |
|  2| 123.123.123.1|  2   |
+---+--------------+------+
```

>嗯，从这个栗子应该就很容易理解并且会写一个udaf了。

### 1.3 hive udf/udaf
> 很多时候，我们想用udf，或者udaf，结果发现网上有实现类似功能的代码，但是人家是用java写给hive用的，我在spark sql里能用吗？答案是肯定的，spark早在1.1就支持了这个功能。  
上代码：  
```scala
    spark.sql("CREATE TEMPORARY FUNCTION UDAFAll AS 'com.meitu.utils.hive.udaf.UDAFAll'")

    spark.sql(s"select id,UDAFAll(action) from tb group by id").show()
```
>没错，就是这么简单，倒入hive-udf的jar包，然后用这种方式注册，然后就可以快乐的在sql里使用udf啦。
## 2.python
> PySpark当然也可以使用udf,但是在使用和性能上还是和scala有写不用的。  
这篇博客讲的挺好的：[How to Use Scala UDF and UDAF in PySpark](http://www.cyanny.com/2017/09/15/spark-use-scala-udf-udaf-in-pyspark/)  
有一点比较流弊的是，强调一下，PySpark可以调用Scala或Java编写的 udf。  
这一块借用这篇[博客](http://blog.cloudera.com/blog/2017/02/working-with-udfs-in-apache-spark/)的代码
### 2.1 udf
> 示例代码如下，[完整代码](https://github.com/curtishoward/sparkudfexamples/tree/master/python-udf)
```python
df = sqlContext.read.json("temperatures.json")
 
df.registerTempTable("citytemps")
# Register the UDF with our SQLContext
 
sqlContext.registerFunction("CTOF", lambda degreesCelsius: ((degreesCelsius * 9.0 / 5.0) + 32.0))
sqlContext.sql("SELECT city, CTOF(avgLow) AS avgLowF, CTOF(avgHigh) AS avgHighF FROM citytemps").show()
```
### 2.2 udaf
> 很遗憾：UDAF now only supports defined in Scala and Java(spark 2.0)  
udaf必须继承类UserDefinedAggregateFunction，所以在scala或者java里写，在pyspark里调用吧。
### 2.3 hive udf/udaf
```python
sqlContext.sql("CREATE TEMPORARY FUNCTION UDAFAll AS 'com.meitu.utils.hive.udaf.UDAFAll'")

sqlContext.sql(s"select id,UDAFAll(action) from tb group by id").show()

```
> 我觉得OK  
待测试！
### 2.4 scala udf from python
> pyspark使用scala写的udf代码如下。[完整代码](https://github.com/curtishoward/sparkudfexamples/blob/master/scala-udaf-from-python/scala-udaf-from-python.py)
```scala
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Scala UDAF from Python example").getOrCreate()

df = spark.read.json("inventory.json")
df.createOrReplaceTempView("inventory")

spark.sparkContext._jvm.com.cloudera.fce.curtis.sparkudfexamples.scalaudaffrompython.ScalaUDAFFromPythonExample.registerUdf()

spark.sql("SELECT Make, SUMPRODUCT(RetailValue,Stock) as InventoryValuePerMake FROM inventory GROUP BY Make").show()
```

## 3.总结
>scala、java、python三只语言，spark和pyspark，还有包括使用hive-udf，总结了那么多，基本涵盖了spark sql开发中关于udf使用的知识点。  
既然udf都是通用的，那么抽出来作为一个工具jar包，还是有点价值的嘛，
放另一个项目git链接  
[hive-udfs](https://github.com/luyao618/hive-udfs)