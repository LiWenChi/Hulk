package demo

import org.apache.spark.sql.{ForeachWriter, SparkSession}
import configure.LoadConfigure
/**
  * Structured Streaming demo
  */
object StructuredStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master("local[*]")
      .appName("test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val lines = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", LoadConfigure.getBrokers())
      .option("group.id", "g1")
      .option("subscribe", "test-topic")
      .load().selectExpr("CAST(value AS STRING)").as[String]
    val ds = lines.writeStream.outputMode("update")
      .format("console").start()

    ds.awaitTermination()
  }
}
