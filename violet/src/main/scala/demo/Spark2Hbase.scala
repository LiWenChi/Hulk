package demo

import java.util

import configure.LoadConfigure
import utils.HbaseUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object Spark2Hbase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("test")
      .config("spark.memory.useLegacyMode", "false")
      .config("spark.shuffle.memoryFraction", "0.6")
      .config("spark.dynamicAllocation.enabled", "false")
      .config("spark.shuffle.file.buffer", "64k")
      .config("spark.reducer.maxSizeInFlight", "96m")
      .config("spark.sql.shuffle.partitions", "60")
      .config("spark.default.parallelism", "180")
      .config("spark.seriailzer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.streaming.release.num.duration", "2")
      .config("spark.streaming.concurrentJobs", 1)      // 并发job数
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val ssc = new StreamingContext(spark.sparkContext,Seconds(10))
    val kafkaParams = getkafkaParms()
    val topicArray = new util.ArrayList[String]()
    topicArray.add("topic-test")

    val dstream:InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream( ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topicArray,kafkaParams))

    dstream.foreachRDD((rdd,time)=>{
      println(time)
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(partitionIterator=>{
          val dataMsg: ArrayBuffer[(String, AnyRef)] = ArrayBuffer[(String,AnyRef)]()
          dataMsg += (("k1", "v1"))
          dataMsg += (("k2", "v2"))
          partitionIterator.foreach(record=>{
            println(record.toString())
          })
          HbaseUtil.init("tb_test")
          HbaseUtil.put("rk01","cf",dataMsg)
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

  def getkafkaParms(): util.HashMap[String,Object] = {
    val kafkaParam = new util.HashMap[String,Object]()
    val brokers = LoadConfigure.getBrokers().toString
    kafkaParam.put( "metadata.broker.list" , brokers )
    kafkaParam.put ( "bootstrap.servers" , brokers )
    kafkaParam.put( "group.id" , "groupid_test" )
    kafkaParam.put( "max.partition.fetch.bytes" , "50000000" )
    kafkaParam.put( "key.deserializer" , "org.apache.kafka.common.serialization.StringDeserializer" )
    kafkaParam.put( "value.deserializer" , "org.apache.kafka.common.serialization.StringDeserializer" )
    kafkaParam
  }
}

