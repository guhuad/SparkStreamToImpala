package com.jaeyeong.datalake.datapipeline.ingest

import com.jaeyeong.datalake.datapipeline.bean.{MysqlSBRBean, ResultBean}
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{OffsetRange, _}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import com.alibaba.fastjson.JSON
import com.jaeyeong.datalake.datapipeline.utils.{OffsetUtil, SqlContants}
import org.apache.spark.sql.SQLContext

import scala.collection.mutable
import scala.collection.mutable.Map
object SparkData extends App with Serializable {

    //1.创建SrearmingContext
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    sc.setCheckpointDir("./kafka")
 // val sqlContext = new SQLContext(sc)
    val ssc = new StreamingContext(sc, Seconds(20))

    //准备连接Kafka的参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "172.17.20.104:9092,172.17.20.105:9092,172.17.20.106:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )


  val rcConf_active =  ConfigFactory.load("kafka")
  val rcConf = ConfigFactory.load(rcConf_active.getString("source.active"))
  val srcConfigParams = Map[String, Object]()
  this.srcConfigParams("bootstrap.servers") = rcConf.getString("source.kafka.bootstrap_servers")
  this.srcConfigParams("key.deserializer") = classOf[StringDeserializer]
  this.srcConfigParams("value.deserializer") = classOf[StringDeserializer]
  this.srcConfigParams("group.id") = rcConf.getString("source.kafka.group_id")
  this.srcConfigParams("auto.offset.reset") = rcConf.getString("source.kafka.auto_offset_reset")
  this.srcConfigParams("enable.auto.commit") = (false: java.lang.Boolean)


//  val topics = Array(rcConf.getString("source.kafka.topic"))
val topics = Array(rcConf.getString("source.kafka.topic"))
  val configPath = "source.kafka.custom_from_offsets"
  if(rcConf.hasPath(configPath)) {
    val fromOffsetConfList = rcConf.getConfigList(configPath)
    val unit = List(fromOffsetConfList).foreach(of => {
      val string = of.get(0).toString
      string
    })

    val test_int = 3
  }

    //2.使用KafkaUtil连接Kafak获取数据
    //注意:
    //如果MySQL中没有记录offset,则直接连接,从latest开始消费
    //如果MySQL中有记录offset,则应该从该offset处开始消费
   // val offsetMap: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()
   // var offsetMap: mutable.Map[TopicPartition, Long] = (new TopicPartition("joymeo_mysql_dev", 0) -> 782326)
    val offsetMap: mutable.Map[TopicPartition, Long] = OffsetUtil.getOffsetMap(rcConf.getString("source.kafka.group_id"), rcConf.getString("source.kafka.topic"))
 // offsetMap2.foreach(of=>{
 //   offsetMap.put(of._1,of._2)
 // })
   // println("offset 从 ："+offsetMap+" 开始")
    val recordDStream: InputDStream[ConsumerRecord[String, String]] = {
      if (offsetMap.size > 0) { //有记录offset
        println("MySQL中记录了offset,则从该offset处开始消费"+offsetMap)
        KafkaUtils.createDirectStream[String, String](ssc,
          LocationStrategies.PreferConsistent, //位置策略,源码强烈推荐使用该策略,会让Spark的Executor和Kafka的Broker均匀对应
          ConsumerStrategies.Subscribe[String, String](topics, srcConfigParams, offsetMap)) //消费策略,源码强烈推荐使用该策略
      } else { //没有记录offset
        println("没有记录offset,则直接连接,从latest开始消费")
        // /export/servers/kafka/bin/kafka-console-producer.sh --broker-list node01:9092 --topic  spark_kafka
        KafkaUtils.createDirectStream[String, String](ssc,
          LocationStrategies.PreferConsistent, //位置策略,源码强烈推荐使用该策略,会让Spark的Executor和Kafka的Broker均匀对应
          ConsumerStrategies.Subscribe[String, String](topics, srcConfigParams)) //消费策略,源码强烈推荐使用该策略
      }
    }


    recordDStream.foreachRDD(rdd => {
      if (rdd.count() > 0) { //当前这一时间批次有数据
        rdd.foreach(record => {
          val mysqlBean: MysqlSBRBean = JSON.parseObject(record.value(), classOf[MysqlSBRBean])
            println("接收到的Kafk发送过来的数据为:" + record)


          try if ("INSERT" == mysqlBean.`type`) { // Contants.createDb(mysqlBean);
            val resultBean =  ResultBean(mysqlBean.database,SqlContants.getUpsertIntoSql(mysqlBean),mysqlBean.table,mysqlBean.`type`)
            SqlContants.sqlExecute(resultBean)
          }
          else if ("UPDATE" == mysqlBean.`type`) {
            val resultBean =  ResultBean(mysqlBean.database,SqlContants.getUpsertIntoSql(mysqlBean),mysqlBean.table,mysqlBean.`type`)
            SqlContants.sqlExecute(resultBean)
          }else if ("DELETE" == mysqlBean.`type`) {
            val resultBean =  ResultBean(mysqlBean.database,SqlContants.getDeleteSql(mysqlBean),mysqlBean.table,mysqlBean.`type`)
            SqlContants.sqlExecute(resultBean)
          }else if ("ALTER" == mysqlBean.`type`) {
            val resultBean =  ResultBean(mysqlBean.database,SqlContants.getAlterSql(mysqlBean),mysqlBean.table,mysqlBean.`type`)
         //   println("接收到的Kafk发送过来的数据为:" + record)
            SqlContants.sqlExecute(resultBean)
            println("修改表结构:" + resultBean.toString)
          }
          catch {
            case e: Exception =>
              e.printStackTrace()
          }

           }

        )

        //接收到的Kafk发送过来的数据为:ConsumerRecord(topic = spark_kafka, partition = 2, offset = 18, CreateTime = 1601332815296, checksum = 2190377400, serialized key size = -1, serialized value size = 2, key = null, value = aa)
        //注意:通过打印接收到的消息可以看到,里面有我们需要维护的offset,和要处理的数据
        //接下来可以对数据进行处理....或者使用transform返回和之前一样处理
        //处理数据的代码写完了,就该维护offset了,那么为了方便我们对offset的维护/管理,spark提供了一个类,帮我们封装offset的数据
        val offsetRanges: Array[OffsetRange] = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        for (o <- offsetRanges) {
       //   println(s"topic=${o.topic},partition=${o.partition},fromOffset=${o.fromOffset},untilOffset=${o.untilOffset}")
        }
        //手动提交offset,默认提交到Checkpoint中
        //recordDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        //实际中偏移量可以提交到MySQL/Redis中
        OffsetUtil.saveOffsetRanges(rcConf.getString("source.kafka.group_id"), offsetRanges)



      }
    })

    val lineDStream: DStream[String] = recordDStream.map(_.value()) //_指的是ConsumerRecord
    val wrodDStream: DStream[String] = lineDStream.flatMap(_.split(" ")) //_指的是发过来的value,即一行数据
    val wordAndOneDStream: DStream[(String, Int)] = wrodDStream.map((_, 1))
    val result: DStream[(String, Int)] = wordAndOneDStream.updateStateByKey(updateFunc)
   // result.print()
    ssc.start() //开启
    ssc.awaitTermination() //等待优雅停止


  def updateFunc(currentValues: Seq[Int], historyValue: Option[Int]): Option[Int] = {
    val result = currentValues.sum + historyValue.getOrElse(0)
    Some(result)
  }

  }



