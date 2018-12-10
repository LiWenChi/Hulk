package up


import java.util

import configure.LoadConfigure
import utils.JsonUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * sparkSQL同步hive表中的数据
  */
object SparkScala {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master(args(0))
      .enableHiveSupport()
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
      .config("spark.streaming.concurrentJobs", 1)
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val kafkaParams = getkafkaParms()
    val topicArray = new util.ArrayList[String]()
    topicArray.add("study")

    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe(topicArray, kafkaParams))

    val schemaString = "env time exec_time user_id topic_id course_id section_id cource_name section_name topic_name folw_id token client_id api in out error"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    dstream.map(_.value()).foreachRDD(rdd => {
      if (!rdd.isEmpty()) {
        val hive = rdd.map(line => {
          val splits = line.split("\\s\\{")
          val json1 = "{" + splits(1)
          val json = JsonUtil.getObjectFromJson(json1)
          Row(json.getString("env"), json.getString("time"), json.getString("exec_time"), json.getString("user_id")
            , json.getString("topic_id"), json.getString("course_id"), json.getString("section_id"), json.getString("cource_name")
            , json.getString("section_name"), json.getString("topic_name"), json.getString("folw_id"), json.getString("token"), json.getString("client_id")
            , json.getString("api"), json.getString("in"), json.getString("out"), json.getString("error"))
        })
        spark.createDataFrame(hive,schema).createOrReplaceTempView("people")
        //val results = spark.sql("SELECT * FROM people")
        //results.show()
        spark.sql("insert into student.hivepeople select * from people")
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def getkafkaParms(): util.HashMap[String, Object] = {
    val kafkaParam = new util.HashMap[String, Object]()
    val brokers = LoadConfigure.getBrokers().toString
    kafkaParam.put( "metadata.broker.list", brokers)
    kafkaParam.put( "bootstrap.servers", brokers)
    kafkaParam.put( "group.id", "cm")
    kafkaParam.put( "max.partition.fetch.bytes", "50000000")
    kafkaParam.put( "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParam.put( "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParam.put( "auto.offset.reset","earliest" )
    kafkaParam
  }
}
