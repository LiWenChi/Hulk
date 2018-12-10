package up


import java.util

import configure.LoadConfigure
import utils.HttpUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * 流式计算：学习系统url调用监控，post rest api上报结果
  */
object StudyApiAlertStreaming {
  /*
  * 报警规则：
  * 1､ use_time > 1s
  * 2､ error出现就报
  * 3､ 1分钟出现三次warn就报警
  * 4､ url 前20个，现在5分钟，前5分钟，  qps 大于2倍
  * 5､ 报警频率控制
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("yarn")
      .enableHiveSupport()
      .appName("StudyApiAlertStreaming")
      .config("spark.seriailzer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val kafkaParams = {
      val kafkaParam = new util.HashMap[String, Object]()
      val brokers = LoadConfigure.getBrokers().toString
      kafkaParam.put("metadata.broker.list", brokers)
      kafkaParam.put("bootstrap.servers", brokers)
      kafkaParam.put("group.id", "rpc-study-1")
      kafkaParam.put("max.partition.fetch.bytes", "50000000")
      kafkaParam.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      kafkaParam.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      kafkaParam
    }
    val topicArray = new util.ArrayList[String]()
    topicArray.add("rpc-study")

    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topicArray, kafkaParams))
    dstream.map(_.value()).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(partitionIterator => {
          partitionIterator.foreach(line => {
            try {
              println("=========================")
              println(line)
              println("=========================")
              //              val ar = line.split("##")
              //              if( ar.length < 8){
              //                //do nothing
              //                println("日志格式不匹配")
              //              }else{
              //                val level = ar(2).toLowerCase
              //                val useTime = ar(6).toInt
              //                val url = ar(8)
              //
              //                if (level == "error") {
              //                  //send msg
              //                  alertRestApi(line)
              //                } else if (useTime > 1000) {
              //                  alertRestApi(line)
              //                } else {
              //                  //do nothing
              //                }
              //              }
            } catch {
              case e: Exception => println(e)
            }

          })
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def alertRestApi(msg: String): Unit = {
    //    http://sms.ops.classba.com.cn/send_sms.php?phone=13020109928&text=“message”
    //    http://172.16.19.173:8080/basketball/v3.0/new/teamname?name=che&age=20
    val url = "http://172.16.19.173:8080/basketball/v3.0/new/teamname?name=che&age=20" + msg
    HttpUtil.getInstance().doGet(url)
  }
}
