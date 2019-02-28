# 问题
> 本文主要讲述了Spark启动的几种姿势。

对于spark(streaming)程序，我们通常是用shell脚本进行启动，而脚本的调用通常是由crontab或者调度系统例如azkaban定时启动，当然azkaban还有创建依赖等等功能。

如果我们的程序触发条件是由某个主动行为，而非固定时间点或者是依赖某个任务呢？比如我想调个接口就可以启动spark程序？

## 姿势一
> Azkaban API 调用

Azkaban的相关文档：  
https://azkaban.readthedocs.io/en/latest/ajaxApi.html

把Spark程序放到Azkaban上，然后通过Azkaban的api调用，不论是curl也好，还是在java里也好，都可以直接通过接口执行程序。  

举个例子
```
//获取session.id
curl -k -X POST --data "action=login&username=Spark&password=HELLOPWD" https://127.0.0.1:8443
//curl执行azkaban任务
curl -k --get --data 'session.id=b57f1460-e909-4c0b-9324-a694d19fb151' --data 'ajax=executeFlow' --data 'project=Test' --data 'flow=TestFlow' https://127.0.0.1:8443/executor --data 'flowOverride[day]=2019-01-29'
```
## 姿势二
> 通过java 调用yarn api来实现spark程序的调度  

这种方式在网上看到的，没有实际操作过。  
放链接：http://www.cnblogs.com/yy3b2007com/p/10247239.html

## 姿势三
> 通过livy调用，启动spark程序    


不知道livy是什么的，可以看下这篇文章：[Livy：基于Apache Spark的REST服务]()

livy还是非常好用的，除了每次都需要把spark的jar包都要上传到HDFS。但是调用起来很简单，也可以curl调用，还可以curl查看log

举个例子：
```
curl -X POST --data '{"file": "'${jar}'", "className": "'${main_class}'","driverMemory":"'${driver_memory}'","executorMemory":"'${executor_memory}'","numExecutors":'${num_executor}',"executorCores":'${executor_cores}',"queue":"'${queue}'","args":["'${org_key}'","'${team_key}'"],"conf":{"spark.master":"yarn","spark.submit.deployMode":"cluster","spark.app.name":"'${appname}'"}}' -H "Content-Type: application/json" 127.0.0.1:8999/batches
```

## 姿势四：
> 有写公司不用Azkaban，然后姿势二和姿势三也都需要安装或者运维配合。那就自己写一个简单的

**举个例子**  ：  
Scala + Redis + 外部命令  
Scala程序一直监听这Redis里的队列，只要队列出现数据，就会消费数据，然后利用外部命令执行shell脚本。
Scala执行外部命令很简单，导入scala.sys.process._包，然后""加!就可以了

```
    val config: java.util.Map[String, String] = PropUtils.load("redis.properties")

    val host = config.get("host")
    val port = config.get("port").toInt
    val password = config.get("password")
    val db = config.get("db").toInt
    val channel = signal.get("channel")

    val jedis = new Jedis(host,port)
    jedis.auth(password) //密码
    jedis.select(db) //选择数据库
    
     try {
      while (true){
        val msg = jedis.lpop("Test_Q")
        if(msg != null ){
          var jsonobj:JSONObject = null
          try{
            jsonobj = JSON.parseObject(msg)
          }catch {
            case e:Exception => {
              logger.error("[ERROR]:解析信号异常，无法解析json：${msg}  at " + DateFunctions.NowDate())
            }
          }
          //执行命令
          executor(jsonobj)
        }else{
          Thread.sleep(1000L)
        }
      }
    }catch {
      case e:Exception =>
        logger.error("[ERROR]:信号处理程序 循环结束 at " + DateFunctions.NowDate() + "exception：" +e)
    }finally {
      logger.error("[ERROR]:信号处理程序结束 at " + DateFunctions.NowDate())
    }

    jedis.close()
    
    
    def executor(jsonobj: JSONObject): Unit = {
        ...//可以按业务处理得到脚本参数
        import scala.sys.process._
        "sh bin/start.sh ${params}" !
        ...
    }
```

## 有其他方式欢迎评论补充

