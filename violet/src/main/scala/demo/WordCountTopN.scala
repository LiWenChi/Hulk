package demo

import org.apache.spark.sql.SparkSession

/**
  * Hello world
  */
object WordCountTopN  extends App{
  def analyzeParam(str: Array[String]): Option[List[String]] = {
    var param = None: Option[List[String]]
    if (str.length > 0) {
      param = Some(str(0).split(",").toList)
    }

    if (param.isEmpty) {
      println("the current parameter is empty")
    } else {
      param.get.foreach(println)
    }
    param
  }

  val param = analyzeParam(args)
  val spark = SparkSession.builder().appName("wordcount").master("local[*]")
    .config("spark.memory.useLegacyMode","true")
    .config("spark.shuffle.memoryFraction","0.6")
    .config("spark.shuffle.file.buffer","64k")
    .config("spark.reducer.maxSizeInFlight","96m")
    .config("spark.sql.shuffle.partitions","60")
    .config("spark.default.parallelism","180")
    .config("spark.seriailzer","org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  val filePath = "d:/kevin/tmp/wc.txt"
  val N = if (param.isEmpty) 3 else param.get(0).toInt
  val rdd = spark.sparkContext.textFile(filePath)
    .filter(line => !line.isEmpty && !line.trim.equals(""))
    .flatMap(words => {
      words.replace("?", "").replace("http", "www").split(" ")
    })
    .map(word => {
      (word, 1)
    })
    .reduceByKey(_ + _)
    .top(N)(ord = new Ordering[(String, Int)]() {
      //实现ordering类的比较函数
      override def compare(x: (String, Int), y: (String, Int)): Int = {
        //先按照出现的次数排序
        //如果次数一样就按单词排序
        val tmp = x._2.compare(y._2)
        if (tmp != 0) tmp else x._1.compare(y._1)
      }

    })
    .foreach(println)

  spark.stop()
}
