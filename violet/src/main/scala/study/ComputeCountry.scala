package study

import java.sql.Connection
import redis.clients.jedis.Jedis
import java.util

import configure.LoadConfigure
import utils._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable.ArrayBuffer

/** @author taoyixing
  *         一、全国纬度（统计总和）
  *         1、试题（精确到阶段-知识点）
  *         使用该试题的总人数、答对总人数、打错总人数、不会总人数、看过解析人数、看过解析答对数、看过解析打错数、看过解析不会数
  *         2、课程
  *         使用课程的总人数、做题数、做对题数、做错题数、不会题数、看过解析答对数、看过解析打错数、看过解析不会数、答题总用时单位为秒
  *         3、专题
  *         进入该专题的总人数、完成该专题的总人数、专题下的做题数、专题下做对的题数、专题下做错的题数、专题下不会的题数、看过解析答对数、看过解析打错数、看过解析不会数、专题下答题总用时单位为秒、完成专题的总时间
  *         4、知识点（精确到阶段）
  *         进入该知识点的总人数、完成该知识点的总人数、知识点下的做题数、知识点下做对的题数、知识点下做错的题数、知识点下不会的题数、看过解析答对数、看过解析打错数、看过解析不会数、知识点下答题总用时单位为秒、知识点完成总时间、知识点的总能力值、知识点对应掌握情况的人数
  *         5、阶段
  *         进入该阶段的总人数、完成该阶段的总人数、阶段下的做题数、阶段下做对的题数、阶段下做错的题数、阶段下不会的题数、看过解析答对数、看过解析打错数、看过解析不会数、阶段下答题总用时单位为秒、完成阶段的总时间
  *         6、能力纬度（精确到阶段）
  *         触发该能力纬度的总人数、该能力纬度下的做题数、该能力纬度下做对的题数、该能力纬度下做错的题数、该能力纬度下不会的题数、看过解析答对数、看过解析打错数、看过解析不会数、能力纬度值
  */
object ComputeCountry {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .master(args(0))
      .appName("ComputeCountry")
      .config("spark.memory.useLegacyMode", "false")
      .config("spark.shuffle.memoryFraction", "0.6")
      .config("spark.dynamicAllocation.enabled", "false")
      .config("spark.shuffle.file.buffer", "64k")
      .config("spark.reducer.maxSizeInFlight", "96m")
      .config("spark.sql.shuffle.partitions", "60")
      .config("spark.default.parallelism", "180")
      .config("spark.seriailzer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.streaming.release.num.duration", "2")
      .config("spark.streaming.concurrentJobs", 1) // 并发job数
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val kafkaParams = getkafkaParms()
    val topicArray = new util.ArrayList[String]()
    topicArray.add("study2")

    val dstream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,

      ConsumerStrategies.Subscribe(topicArray, kafkaParams))

    dstream.foreachRDD((rdd, time) => {
      println(time)
      if (!rdd.isEmpty()) {
        rdd.foreachPartition(partitionIterator => {
          partitionIterator.foreach(line => {
            val conn = C3p0Pools.getConnection()
            val jedis = JedisUtil.getJedis(0)
            val normalFlag = DataUtil.isNormative(line.value())
            if (normalFlag) {
              println(line.value())
              handleCourseNationWide(line.value(),conn,jedis)
              handleQuestionNationWide(line.value(),conn,jedis)
              handleFactorCodesNationWide(line.value(),conn,jedis)
              handleTopicNationWide(line.value(),conn,jedis)
              handleSectionNationWide(line.value(),conn,jedis)
              handleTagCodeNationWide(line.value(),conn,jedis)
            } else {
              println("异常日志格式")
            }

            //释放mysql连接
            C3p0Pools.closeConnecton(conn)
            // 释放redis连接
            JedisUtil.retJedis(jedis)

          })

        })
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }

  def getkafkaParms(): util.HashMap[String, Object] = {
    val kafkaParam = new util.HashMap[String, Object]()
    val brokers = LoadConfigure.getBrokers().toString
    kafkaParam.put("metadata.broker.list", brokers)
    kafkaParam.put("bootstrap.servers", brokers)
    kafkaParam.put("group.id", "g3")
    kafkaParam.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParam.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaParam
  }

  def handleCourseNationWide(line: String , conn : Connection , jedis : Jedis ): Unit = {
    // 1.1.5 课程

    //初始化

    val params = ArrayBuffer[Any]()

    // 解析获得的一行数据为Json对象
    val jsonStr = line.substring(line.indexOf('{'))
    val json = JsonUtil.getObjectFromJson(jsonStr)


    // 获取data的信息 以及src_type
    val dataObj = JsonUtil.getObjectFromJson(json.get("data").toString)
    val srcType = dataObj.get("src_type")

    // 表 study_archive_nationwide_courses 用于试题相关的归档
    // 三个sql 分别用于数据库 查询 插入 更新
    val searchSql = "select * from study_archive_nationwide_courses where courseId = ? "
    val insertSql = "insert into study_archive_nationwide_courses (courseId) values (?) "
    val updateSql = " update study_archive_nationwide_courses  set courseEntryNumbers = ? , answerNumbers = ? , " +
      "answerRightNumbers = ? , answerWrongNumbers = ? , incompetentNumbers = ? , " +
      "viewAnalysisNumbers = ? , viewRightNumbers = ? , viewWrongNumbers = ? , viewIncompetentNumbers = ? , " +
      "courseAnswerTime = ? where courseId = ? "

    // 相关sql参数  联合主键
    var compositedIdParam = ArrayBuffer[Any](dataObj.get("course_id"))

    //获取当前dataObj中的数据
    val accountId = dataObj.get("account_id").toString
    val courseId = dataObj.get("course_id").toString
    val accountId_course_id_md5 = Md5Util.md5(accountId + "_course_id")



    // 判断src_type
    if (srcType == "SUBMIT_QUESTION") {
      // 根据i更新统计结果
      params ++= compositedIdParam

      val rs = C3p0Pools.query(conn, searchSql, params.toArray[Any])

      // 默认记录在数据库不存在
      var flag = false

      while (rs.next()) {
        if (!flag) {
          flag = true
        }
        // 从rs中获取当前课程id的变量
        // courseId ,courseEntryNumbers,answerNumbers ,answerRightNumbers ,answerWrongNumbers ,incompetentNumbers ,viewAnalysisNumbers ,
        // viewRightNumbers ,viewWrongNumbers ,viewIncompetentNumbers ,courseAnswerTime

        var courseEntryNumbers = rs.getInt("courseEntryNumbers")
        var answerNumbers = rs.getInt("answerNumbers")
        var answerRightNumbers = rs.getInt("answerRightNumbers")
        var answerWrongNumbers = rs.getInt("answerWrongNumbers")
        var incompetentNumbers = rs.getInt("incompetentNumbers")
        var viewAnalysisNumbers = rs.getInt("viewAnalysisNumbers")
        var viewRightNumbers = rs.getInt("viewRightNumbers")
        var viewWrongNumbers = rs.getInt("viewWrongNumbers")
        var viewIncompetentNumbers = rs.getInt("viewIncompetentNumbers")
        var courseAnswerTime = rs.getInt("courseAnswerTime")



        // 判断redis是否存在上一条的课程记录
        if (jedis.get(accountId_course_id_md5) == null) {
          // 进入课程总人次 + 1
          courseEntryNumbers += 1

          // 写入缓存
          jedis.set(accountId_course_id_md5, courseId)

        } else {

          if (jedis.get(accountId_course_id_md5) != courseId) {
            // 进入课程总人次 + 1
            courseEntryNumbers += 1
            // redis 更新缓存
            jedis.set(accountId_course_id_md5, courseId)

          }
        }

        // 课程下做题总用时（秒）
        courseAnswerTime += dataObj.getIntValue("cost_time")

        // 课程下做题总数
        answerNumbers += 1

        // 课程下做对题数, 课程下做错题数, 课程下不会题数

        val isRight = dataObj.get("is_right").toString
        isRight match {
          case "0" => answerWrongNumbers += 1
          case "1" => answerRightNumbers += 1
          case "2" => incompetentNumbers += 1
        }

        // 课程下看过解析做题总数、做对、做错、不会

        val isViewAnalyze = dataObj.get("is_view_analyze").toString
        isViewAnalyze match {
          case "0" =>
            println("未看过解析")
          // 看过解析统计: 答对，答错，不会的人次
          case "1" =>
            // 看过解析总人数
            viewAnalysisNumbers += 1
            isRight match {
              case "0" => viewWrongNumbers += 1
              case "1" => viewRightNumbers += 1
              case "2" => viewIncompetentNumbers += 1
            }

        }

        // 更新数据库

        params.clear()
        params ++= ArrayBuffer[Any](courseEntryNumbers, answerNumbers, answerRightNumbers, answerWrongNumbers,
          incompetentNumbers, viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers,
          courseAnswerTime)
        params ++= compositedIdParam
        val rs_data = C3p0Pools.execute(updateSql, params.toArray[Any], conn)
        if (rs_data.equals(1))
          println("当前的 课程记录 归档成功")

      }

      // 如果记录数据库中真不存在 初始化插入
      if (!flag) {
        params.clear()
        params ++= compositedIdParam
        val rsInsert = C3p0Pools.execute(insertSql, params.toArray[Any], conn)
        if (rsInsert.equals(1)) {
          println("数据库新增 该课程记录的第一条数据 成功")
        }

        // 对进入的第一条数据 进行数据库更新
        // courseId ,courseEntryNumbers,answerNumbers ,answerRightNumbers ,answerWrongNumbers ,incompetentNumbers
        // ,viewAnalysisNumbers ,viewRightNumbers ,viewWrongNumbers ,viewIncompetentNumbers ,courseAnswerTime

        var (courseEntryNumbers, answerNumbers, answerRightNumbers, answerWrongNumbers, incompetentNumbers, viewAnalysisNumbers,
        viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers, courseAnswerTime) = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

        courseEntryNumbers += 1

        answerNumbers += 1

        val isRight = dataObj.get("is_right").toString
        isRight match {
          case "0" => answerWrongNumbers += 1
          case "1" => answerRightNumbers += 1
          case "2" => incompetentNumbers += 1
        }

        // 课程下看过解析做题总数、做对、做错、不会

        val isViewAnalyze = dataObj.get("is_view_analyze").toString
        isViewAnalyze match {
          case "0" =>
            println("该课程下做该题未看过解析")

          // 看过解析统计: 答对，答错，不会的人次
          case "1" =>
            // 看过解析总人数
            viewAnalysisNumbers += 1
            isRight match {
              case "0" => viewWrongNumbers += 1
              case "1" => viewRightNumbers += 1
              case "2" => viewIncompetentNumbers += 1
            }

        }

        courseAnswerTime += dataObj.getIntValue("cost_time")


        // 更新数据库

        params.clear()
        params ++= ArrayBuffer[Any](courseEntryNumbers, answerNumbers, answerRightNumbers, answerWrongNumbers,
          incompetentNumbers, viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers,
          courseAnswerTime)
        params ++= compositedIdParam
        val rs_data = C3p0Pools.execute(updateSql, params.toArray[Any], conn)
        if (rs_data.equals(1)) {
          println("该课程记录 归档成功")
        }
        jedis.set(accountId_course_id_md5, courseId)

      }
    }

  }

  def handleTopicNationWide(line: String , conn : Connection , jedis : Jedis): Unit = {
    // 1.1.4 专题

    //初始化
    val params = ArrayBuffer[Any]()


    // 解析每一行数据为Json对象
    val jsonStr = line.substring(line.indexOf('{'))
    val json = JsonUtil.getObjectFromJson(jsonStr)

    // 获取时间
    val time = json.get("time").toString
    val timeStamp: Long = DateUtil.str2ts(time)
    val timeStampStr = timeStamp.toString


    // 获取data
    val dataObj = JsonUtil.getObjectFromJson(json.get("data").toString)
    val srcType = dataObj.get("src_type")

    // 表 study_archive_nationwide_topics 用于专题相关的归档
    val searchSql = "select * from study_archive_nationwide_topics where courseId = ? and topicId = ? "
    val insertSql = "insert into study_archive_nationwide_topics (courseId , topicId ) values (? , ?) "
    val updateSql = " update study_archive_nationwide_topics  set topicEntryNumbers = ? , topicCompletionNumbers = ? , " +
      "answerNumbers = ? , answerRightNumbers = ? , answerWrongNumbers = ? , incompetentNumbers = ? , " +
      "viewAnalysisNumbers =" + " ? , viewRightNumbers = ? , viewWrongNumbers= ? , viewIncompetentNumbers = ? ," +
      "topicAnswerTime = ? , " + "topicCompletionTime = ? where courseId = ? and topicId = ? "

    // 相关sql参数
    var compositedIdParam = ArrayBuffer[Any](dataObj.get("course_id"), dataObj.get("topic_id"))


    //获取dataObj中的数据
    val accountId = dataObj.get("account_id").toString
    val topicId = dataObj.get("topic_id").toString
    val courseId = dataObj.get("course_id").toString

    val accountId_course_topic_id_md5 = Md5Util.md5(accountId + "_" + courseId + "_topic_id")
    val accountId_course_topic_id_first_time_md5: String = Md5Util.md5(accountId + "_" + courseId + "_" + topicId + "_first_time")


    // 判断src_type
    if (srcType == "SUBMIT_QUESTION") {
      // 根据i更新统计结果
      params ++= compositedIdParam

      val rs = C3p0Pools.query(conn, searchSql, params.toArray[Any])

      // 默认记录在数据库不存在
      var flag = false

      while (rs.next()) {
        if (!flag) {
          flag = true
        }

        // 从rs中获取当前topic的相关参数
        var topicEntryNumbers = rs.getInt("topicEntryNumbers")
        val topicCompletionNumbers = rs.getInt("topicCompletionNumbers")
        var answerNumbers = rs.getInt("answerNumbers")
        var answerRightNumbers = rs.getInt("answerRightNumbers")
        var answerWrongNumbers = rs.getInt("answerWrongNumbers")
        var incompetentNumbers = rs.getInt("incompetentNumbers")
        var viewAnalysisNumbers = rs.getInt("viewAnalysisNumbers")
        var viewRightNumbers = rs.getInt("viewRightNumbers")
        var viewWrongNumbers = rs.getInt("viewWrongNumbers")
        var viewIncompetentNumbers = rs.getInt("viewIncompetentNumbers")
        var topicAnswerTime = rs.getInt("topicAnswerTime")
        val topicCompletionTime = rs.getInt("topicCompletionTime")




        // 判断redis是否存在上一条的专题记录
        if (jedis.get(accountId_course_topic_id_md5) == null) {
          // 进入课程总人次 + 1
          topicEntryNumbers += 1

          // 写入缓存
          jedis.set(accountId_course_topic_id_md5, topicId)
          jedis.set(accountId_course_topic_id_first_time_md5, timeStampStr)

        }

        // 专题下做题总用时（秒）
        topicAnswerTime += dataObj.getIntValue("cost_time")

        // 专题下做题总数
        answerNumbers += 1

        // 专题下做对题数, 专题下做错题数, 专题下不会题数

        val isRight = dataObj.get("is_right").toString
        isRight match {
          case "0" => answerWrongNumbers += 1
          case "1" => answerRightNumbers += 1
          case "2" => incompetentNumbers += 1
        }

        // 专题下看过解析做题总数、做对、做错、不会

        val isViewAnalyze = dataObj.get("is_view_analyze").toString
        isViewAnalyze match {
          case "0" =>
            println("该专题下做该道题未看过解析")

          // 看过解析统计: 答对，答错，不会的人次
          case "1" =>
            // 看过解析总人数
            viewAnalysisNumbers += 1
            isRight match {
              case "0" => viewWrongNumbers += 1
              case "1" => viewRightNumbers += 1
              case "2" => viewIncompetentNumbers += 1
            }

        }

        // 更新数据库

        params.clear()
        params ++= ArrayBuffer[Any](topicEntryNumbers, topicCompletionNumbers, answerNumbers, answerRightNumbers, answerWrongNumbers,
          incompetentNumbers, viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers,
          topicAnswerTime, topicCompletionTime)
        params ++= compositedIdParam
        val rs_data = C3p0Pools.execute(updateSql, params.toArray[Any], conn)
        if (rs_data.equals(1)) {
          println("当前的专题记录 归档成功")
        }
      }

      // 如果记录数据库中真不存在 初始化插入
      if (!flag) {
        params.clear()
        params ++= compositedIdParam
        val rsInsert = C3p0Pools.execute(insertSql, params.toArray[Any], conn)
        if (rsInsert.equals(1))
          println("数据库新增 该专题记录的第一条数据 成功")

        // ************ 需要对一条数据进行插入


        var (topicEntryNumbers, topicCompletionNumbers, answerNumbers, answerRightNumbers, answerWrongNumbers,
        incompetentNumbers, viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers,
        topicAnswerTime, topicCompletionTime) = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

        answerNumbers += 1

        topicAnswerTime += dataObj.getIntValue("cost_time")

        // 进入课程总人次 + 1
        topicEntryNumbers += 1


        val isRight = dataObj.get("is_right").toString
        isRight match {
          case "0" => answerWrongNumbers += 1
          case "1" => answerRightNumbers += 1
          case "2" => incompetentNumbers += 1
        }

        // 专题下看过解析做题总数、做对、做错、不会

        val isViewAnalyze = dataObj.get("is_view_analyze").toString
        isViewAnalyze match {
          case "0" =>
            println("未看过解析")

          // 看过解析统计: 答对，答错，不会的人次
          case "1" =>
            // 看过解析总人数
            viewAnalysisNumbers += 1
            isRight match {
              case "0" => viewWrongNumbers += 1
              case "1" => viewRightNumbers += 1
              case "2" => viewIncompetentNumbers += 1
            }

        }


        params.clear()
        params ++= ArrayBuffer[Any](topicEntryNumbers, topicCompletionNumbers, answerNumbers, answerRightNumbers, answerWrongNumbers,
          incompetentNumbers, viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers,
          topicAnswerTime, topicCompletionTime)
        params ++= compositedIdParam
        val rs_data = C3p0Pools.execute(updateSql, params.toArray[Any], conn)
        if (rs_data.equals(1)) {
          println("当前课程归档成功")
        }

        // 写入缓存
        jedis.set(accountId_course_topic_id_md5, topicId)
        jedis.set(accountId_course_topic_id_first_time_md5, timeStampStr)


      }


    }

    // 判断src_type
    if (srcType == "TOPIC_END") {

      if (jedis.get(accountId_course_topic_id_md5) == null) {
        println("专题缓存异常")
        return
      }

      if (jedis.get(accountId_course_topic_id_first_time_md5) == null) {
        println("专题缓存异常")
        return
      }


      params.clear()
      params ++= compositedIdParam

      // 查询
      val rs = C3p0Pools.query(conn, searchSql, params.toArray[Any])

      while (rs.next()) {

        // 从rs中获取当前topic的相关参数
        val topicEntryNumbers = rs.getInt("topicEntryNumbers")
        var topicCompletionNumbers = rs.getInt("topicCompletionNumbers")
        val answerNumbers = rs.getInt("answerNumbers")
        val answerRightNumbers = rs.getInt("answerRightNumbers")
        val answerWrongNumbers = rs.getInt("answerWrongNumbers")
        val incompetentNumbers = rs.getInt("incompetentNumbers")
        val viewAnalysisNumbers = rs.getInt("viewAnalysisNumbers")
        val viewRightNumbers = rs.getInt("viewRightNumbers")
        val viewWrongNumbers = rs.getInt("viewWrongNumbers")
        val viewIncompetentNumbers = rs.getInt("viewIncompetentNumbers")
        val topicAnswerTime = rs.getInt("topicAnswerTime")
        var topicCompletionTime = rs.getInt("topicCompletionTime")


        topicCompletionTime += ((timeStamp - jedis.get(accountId_course_topic_id_first_time_md5).toLong) / 1000).toInt
        topicCompletionNumbers += 1

        // 更新数据库

        params.clear()
        params ++= ArrayBuffer[Any](topicEntryNumbers, topicCompletionNumbers, answerNumbers, answerRightNumbers, answerWrongNumbers,
          incompetentNumbers, viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers,
          topicAnswerTime, topicCompletionTime)
        params ++= compositedIdParam
        val rs_data = C3p0Pools.execute(updateSql, params.toArray[Any], conn)
        if (rs_data.equals(1)) {
          println("当前专题归档成功")
        }

        //销毁缓存
        jedis.del(accountId_course_topic_id_md5)
        jedis.del(accountId_course_topic_id_first_time_md5)

      }
    }

  }

  def handleSectionNationWide(line: String , conn : Connection , jedis : Jedis): Unit = {
    // 1.1.3 阶段


    //初始化

    val params = ArrayBuffer[Any]()


    // 解析每一行数据为Json对象
    val jsonStr = line.substring(line.indexOf('{'))
    val json = JsonUtil.getObjectFromJson(jsonStr)

    // 获取时间
    val time = json.get("time").toString
    val timeStamp: Long = DateUtil.str2ts(time)
    val timeStampStr = timeStamp.toString

    // 获取data
    val dataObj = JsonUtil.getObjectFromJson(json.get("data").toString)
    val srcType = dataObj.get("src_type")

    // 表 study_archive_nationwide_sections 用于阶段相关的归档
    val searchSql = "select * from study_archive_nationwide_sections where courseId = ? and topicId = ? and sectionId = ? "
    val insertSql = "insert into study_archive_nationwide_sections (courseId , topicId , sectionId) values (? , ?, ?) "
    val updateSql = " update study_archive_nationwide_sections  set   sectionEntryNumbers = ?," +
      "sectionCompletionNumbers = ? , answerNumbers = ? , answerRightNumbers = ?, answerWrongNumbers = ? , " +
      "incompetentNumbers = ? , viewAnalysisNumbers = ? , viewRightNumbers = ?,viewWrongNumbers = ? , " +
      "viewIncompetentNumbers = ? , sectionAnswerTime = ? ,  sectionCompletionTime = ? " +
      "where courseId = ? and topicId = ? and sectionId = ? "


    //获取dataObj中的数据
    val accountId = dataObj.get("account_id").toString
    val topicId = dataObj.get("topic_id").toString
    val courseId = dataObj.get("course_id").toString
    val accountId_course_topic_section_id_md5 = Md5Util.md5(accountId + "_" + courseId + "_" + topicId + "_section_id")



    // 判断src_type
    if (srcType == "SUBMIT_QUESTION") {
      // 相关sql参数
      var compositedIdParam = ArrayBuffer[Any](dataObj.get("course_id"), dataObj.get("topic_id"), dataObj.get("module_type"))


      val sectionId = dataObj.get("module_type").toString
      val accountId_course_topic_section_id_first_time_md5: String = Md5Util.md5(accountId + "_" + courseId + "_" + topicId +
        "_" + sectionId + "_first_time")
      // 根据i更新统计结果
      params ++= compositedIdParam

      val rs = C3p0Pools.query(conn, searchSql, params.toArray[Any])

      // 默认记录在数据库不存在
      var flag = false

      while (rs.next()) {
        if (!flag) {
          flag = true
        }

        // 从rs中获取当前topic的相关参数
        var sectionEntryNumbers = rs.getInt("sectionEntryNumbers")
        val sectionCompletionNumbers = rs.getInt("sectionCompletionNumbers")
        var answerNumbers = rs.getInt("answerNumbers")
        var answerRightNumbers = rs.getInt("answerRightNumbers")
        var answerWrongNumbers = rs.getInt("answerWrongNumbers")
        var incompetentNumbers = rs.getInt("incompetentNumbers")
        var viewAnalysisNumbers = rs.getInt("viewAnalysisNumbers")
        var viewRightNumbers = rs.getInt("viewRightNumbers")
        var viewWrongNumbers = rs.getInt("viewWrongNumbers")
        var viewIncompetentNumbers = rs.getInt("viewIncompetentNumbers")
        var sectionAnswerTime = rs.getInt("sectionAnswerTime")
        val sectionCompletionTime = rs.getInt("sectionCompletionTime")




        // 判断redis是否存在上一条的阶段记录
        if (jedis.get(accountId_course_topic_section_id_md5) == null) {
          // 进入课程总人次 + 1
          sectionEntryNumbers += 1

          // 写入缓存
          jedis.set(accountId_course_topic_section_id_md5, topicId)
          jedis.set(accountId_course_topic_section_id_first_time_md5, timeStampStr)

        }

        // 阶段下做题总用时（秒）
        sectionAnswerTime += dataObj.getIntValue("cost_time")

        // 阶段下做题总数
        answerNumbers += 1

        // 阶段下做对题数, 阶段下做错题数, 课阶段下不会题数

        val isRight = dataObj.get("is_right").toString
        isRight match {
          case "0" => answerWrongNumbers += 1
          case "1" => answerRightNumbers += 1
          case "2" => incompetentNumbers += 1
        }

        // 阶段下看过解析做题总数、做对、做错、不会

        val isViewAnalyze = dataObj.get("is_view_analyze").toString
        isViewAnalyze match {
          case "0" =>
            println("未看过解析")

          // 看过解析统计: 答对，答错，不会的人次
          case "1" =>
            // 看过解析总人数
            viewAnalysisNumbers += 1
            isRight match {
              case "0" => viewWrongNumbers += 1
              case "1" => viewRightNumbers += 1
              case "2" => viewIncompetentNumbers += 1
            }

        }

        // 更新数据库

        params.clear()
        params ++= ArrayBuffer[Any](sectionEntryNumbers, sectionCompletionNumbers, answerNumbers, answerRightNumbers, answerWrongNumbers,
          incompetentNumbers, viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers,
          sectionAnswerTime, sectionCompletionTime)
        params ++= compositedIdParam
        val rs_data = C3p0Pools.execute(updateSql, params.toArray[Any], conn)
        if (rs_data.equals(1)) {
          println("当前的 阶段数据 归档成功")
        }
      }

      // 如果记录数据库中真不存在 初始化插入
      if (!flag) {
        params.clear()
        params ++= compositedIdParam
        val rsInsert = C3p0Pools.execute(insertSql, params.toArray[Any], conn)
        if (rsInsert.equals(1))
          println("数据库新增阶段数据成功")

        // ************ 需要对一条数据进行插入


        var (sectionEntryNumbers, sectionCompletionNumbers, answerNumbers, answerRightNumbers, answerWrongNumbers,
        incompetentNumbers, viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers,
        sectionAnswerTime, sectionCompletionTime) = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

        answerNumbers += 1

        sectionAnswerTime += dataObj.getIntValue("cost_time")

        // 进入阶段总人次 + 1
        sectionEntryNumbers += 1


        val isRight = dataObj.get("is_right").toString
        isRight match {
          case "0" => answerWrongNumbers += 1
          case "1" => answerRightNumbers += 1
          case "2" => incompetentNumbers += 1
        }

        // 阶段下看过解析做题总数、做对、做错、不会

        val isViewAnalyze = dataObj.get("is_view_analyze").toString
        isViewAnalyze match {
          case "0" =>
            println("该阶段下 做此题 未看过解析")

          // 看过解析统计: 答对，答错，不会的人次
          case "1" =>
            // 看过解析总人数
            viewAnalysisNumbers += 1
            isRight match {
              case "0" => viewWrongNumbers += 1
              case "1" => viewRightNumbers += 1
              case "2" => viewIncompetentNumbers += 1
            }

        }


        params.clear()
        params ++= ArrayBuffer[Any](sectionEntryNumbers, sectionCompletionNumbers, answerNumbers, answerRightNumbers, answerWrongNumbers,
          incompetentNumbers, viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers,
          sectionAnswerTime, sectionCompletionTime)
        params ++= compositedIdParam
        val rs_data = C3p0Pools.execute(updateSql, params.toArray[Any], conn)
        if (rs_data.equals(1)) {
          println("当前 阶段数据 归档成功")
        }

        // 写入缓存
        jedis.set(accountId_course_topic_section_id_md5, sectionId)
        jedis.set(accountId_course_topic_section_id_first_time_md5, timeStampStr)

      }


    }

    // 判断src_type
    if (srcType == "SECTION_END") {

      val sectionId = dataObj.get("section_id").toString
      val accountId_course_topic_section_id_first_time_md5: String = Md5Util.md5(accountId + "_" + courseId + "_" + topicId +
        "_" + sectionId + "_first_time")


      if (jedis.get(accountId_course_topic_section_id_md5) == null) {
        println("阶段缓存异常")
        return
      }

      if (jedis.get(accountId_course_topic_section_id_first_time_md5) == null) {
        println("阶段缓存异常")
        return
      }


      params.clear()
      var compositedIdParam = ArrayBuffer[Any](dataObj.get("course_id"), dataObj.get("topic_id"), dataObj.get("section_id"))
      params ++= compositedIdParam

      // 查询
      val rs = C3p0Pools.query(conn, searchSql, params.toArray[Any])

      while (rs.next()) {

        // 从rs中获取当前section的相关参数
        val sectionEntryNumbers = rs.getInt("sectionEntryNumbers")
        var sectionCompletionNumbers = rs.getInt("sectionCompletionNumbers")
        val answerNumbers = rs.getInt("answerNumbers")
        val answerRightNumbers = rs.getInt("answerRightNumbers")
        val answerWrongNumbers = rs.getInt("answerWrongNumbers")
        val incompetentNumbers = rs.getInt("incompetentNumbers")
        val viewAnalysisNumbers = rs.getInt("viewAnalysisNumbers")
        val viewRightNumbers = rs.getInt("viewRightNumbers")
        val viewWrongNumbers = rs.getInt("viewWrongNumbers")
        val viewIncompetentNumbers = rs.getInt("viewIncompetentNumbers")
        val sectionAnswerTime = rs.getInt("sectionAnswerTime")
        var sectionCompletionTime = rs.getInt("sectionCompletionTime")

        sectionCompletionTime += ((timeStamp - jedis.get(accountId_course_topic_section_id_first_time_md5).toLong) / 1000).toInt
        sectionCompletionNumbers += 1

        // 更新数据库

        params.clear()
        params ++= ArrayBuffer[Any](sectionEntryNumbers, sectionCompletionNumbers, answerNumbers, answerRightNumbers, answerWrongNumbers,
          incompetentNumbers, viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers,
          sectionAnswerTime, sectionCompletionTime)
        params ++= compositedIdParam
        val rs_data = C3p0Pools.execute(updateSql, params.toArray[Any], conn)
        if (rs_data.equals(1)) {
          println("当前阶段归档成功")
        }

        //销毁缓存
        jedis.del(accountId_course_topic_section_id_md5)
        jedis.del(accountId_course_topic_section_id_first_time_md5)

      }
    }

  }

  def handleTagCodeNationWide(line: String , conn : Connection , jedis : Jedis): Unit = {
    // 1.1.2 知识点

    //初始化

    val params = ArrayBuffer[Any]()


    // 解析每一行数据为Json对象
    val jsonStr = line.substring(line.indexOf('{'))
    val json = JsonUtil.getObjectFromJson(jsonStr)

    // 获取时间
    val time = json.get("time").toString
    val timeStamp: Long = DateUtil.str2ts(time)
    val timeStampStr = timeStamp.toString

    // 获取data
    val dataObj = JsonUtil.getObjectFromJson(json.get("data").toString)
    val srcType = dataObj.get("src_type")

    // 表 study_archive_nationwide_tagCodes 用于知识点相关的归档
    val searchSql = "select * from study_archive_nationwide_tagCodes where courseId = ? and lessonId = ? and topicId = ?" +
      "  and sectionId = ? and tagCodeId = ? "
    val insertSql = "insert into study_archive_nationwide_tagCodes (courseId , lessonId , topicId , sectionId, tagCodeId) values " +
      "(?, ?, ?, ? , ?) "
    val updateSql = " update study_archive_nationwide_tagCodes  set   tagCodeEntryNumbers = ? , " +
      "tagCodeCompletionNumbers = ? , answerNumbers = ? , answerRightNumbers = ? , answerWrongNumbers = ? , " +
      "incompetentNumbers = ? , viewAnalysisNumbers = ? , viewRightNumbers = ? , viewWrongNumbers = ? , " +
      "viewIncompetentNumbers = ? , tagCodeAnswerTime = ? , tagCodeCompletionTime = ? , tagCodeAbilitySum = ? , " +
      "loStatusPASSED = ? , loStatusFAILED = ? , loStatusINPROCESS = ? , loStatusDEALED = ? , " +
      "loStatusNODEALED = ? , loStatusUNCERTAIN = ? , loStatusNOITEM = ? " +
      "where courseId = ? and lessonId = ? and topicId = ? and sectionId = ? and tagCodeId = ?"
    val updateLastTagCodeSql = "update study_archive_nationwide_tagCodes  set tagCodeEntryNumbers =? , " +
      "tagCodeAbilitySum=? , loStatusPASSED = ? , loStatusFAILED = ? , loStatusINPROCESS = ? , loStatusDEALED = ? , " +
      "loStatusNODEALED = ? , loStatusUNCERTAIN = ? , loStatusNOITEM = ? ,tagCodeCompletionTime = ? , tagCodeCompletionNumbers = ? " +
      "where courseId = ? and lessonId = ?  and topicId = ? and sectionId = ? and tagCodeId = ? "





    // 判断src_type
    if (srcType == "SUBMIT_QUESTION") {

      // 相关sql参数
      var compositedIdParam = ArrayBuffer[Any](dataObj.get("course_id"), dataObj.get("lesson_id"), dataObj.get("topic_id"), dataObj.get("module_type")
        , dataObj.get("tag_code"))


      //获取dataObj中的数据
      val accountId = dataObj.get("account_id").toString
      val lessonId = dataObj.get("lesson_id").toString
      val topicId = dataObj.get("topic_id").toString
      val courseId = dataObj.get("course_id").toString
      val sectionId = dataObj.get("module_type").toString
      val tagCodeId = dataObj.get("tag_code").toString
      val questionId = dataObj.get("question_id").toString
      val loStatus = dataObj.get("lo_status").toString
      val ability = dataObj.get("ability").toString


      val accountId_course_topic_section_tagCode_id_md5 = Md5Util.md5(accountId + "_" + courseId + "_" + lessonId + "_" + topicId + "_" + sectionId + "_tagCode_id")
      val accountId_course_topic_section_tagCode_id_first_time_md5 = Md5Util.md5(accountId + "_" + courseId + "_" + lessonId + "_" + topicId +
        "_" + sectionId + "_first_time")
      val accountId_course_topic_section_tagCode_last_ability_md5 = Md5Util.md5(accountId + "_" + courseId + "_" + lessonId + "_" + topicId +
        "_" + sectionId + "_ability")
      val accountId_course_topic_section_tagCode_last_lo_status_md5 = Md5Util.md5(accountId + "_" + courseId + "_" + lessonId + "_" + topicId +
        "_" + sectionId + "_lo_status")
      val accountId_course_topic_section_tagCode_last_question_id_md5 = Md5Util.md5(accountId + "_" + courseId + "_" + lessonId + "_" + topicId +
        "_" + sectionId + "_question_id")

      // 根据i更新统计结果
      params ++= compositedIdParam

      var rs = C3p0Pools.query(conn, searchSql, params.toArray[Any])

      // 默认记录在数据库不存在
      var flag = false

      while (rs.next()) {
        if (!flag) {
          flag = true
        }

        //  tagCodeEntryNumbers , tagCodeCompletionNumbers , answerNumbers , answerRightNumbers ,
        // answerWrongNumbers , incompetentNumbers , viewAnalysisNumbers , viewRightNumbers ,
        // viewWrongNumbers , viewIncompetentNumbers , tagCodeAnswerTime , tagCodeCompletionTime ,
        // tagCodeAbilitySum , loStatusPASSED , loStatusFAILED , loStatusINPROCESS ,
        // loStatusDEALED , loStatusNODEALED , loStatusUNCERTAIN , loStatusNOITEM


        // 从rs中获取当前tagCode的相关参数
        var tagCodeEntryNumbers = rs.getInt("tagCodeEntryNumbers")
        var tagCodeCompletionNumbers = rs.getInt("tagCodeCompletionNumbers")

        var answerNumbers = rs.getInt("answerNumbers")
        var answerRightNumbers = rs.getInt("answerRightNumbers")
        var answerWrongNumbers = rs.getInt("answerWrongNumbers")

        var incompetentNumbers = rs.getInt("incompetentNumbers")
        var viewAnalysisNumbers = rs.getInt("viewAnalysisNumbers")
        var viewRightNumbers = rs.getInt("viewRightNumbers")
        var viewWrongNumbers = rs.getInt("viewWrongNumbers")
        var viewIncompetentNumbers = rs.getInt("viewIncompetentNumbers")

        var tagCodeAnswerTime = rs.getInt("tagCodeAnswerTime")
        var tagCodeCompletionTime = rs.getInt("tagCodeCompletionTime")
        var tagCodeAbilitySum = rs.getDouble("tagCodeAbilitySum")

        var loStatusPASSED = rs.getInt("loStatusPASSED")
        var loStatusFAILED = rs.getInt("loStatusFAILED")
        var loStatusINPROCESS = rs.getInt("loStatusINPROCESS")
        var loStatusDEALED = rs.getInt("loStatusDEALED")
        var loStatusNODEALED = rs.getInt("loStatusNODEALED")
        var loStatusUNCERTAIN = rs.getInt("loStatusUNCERTAIN")
        var loStatusNOITEM = rs.getInt("loStatusNOITEM")


        // 判断redis是否存在上一条的知识点记录


        if (jedis.get(accountId_course_topic_section_tagCode_id_md5) == null) {
          // 进入知识点总人次 + 1
          tagCodeEntryNumbers += 1

          // 写入缓存
          jedis.set(accountId_course_topic_section_tagCode_id_md5, tagCodeId)
          jedis.set(accountId_course_topic_section_tagCode_id_first_time_md5, timeStampStr)
          jedis.set(accountId_course_topic_section_tagCode_last_ability_md5, ability)
          jedis.set(accountId_course_topic_section_tagCode_last_lo_status_md5, loStatus)
          jedis.set(accountId_course_topic_section_tagCode_last_question_id_md5, questionId)

        } else {

          if (jedis.get(accountId_course_topic_section_tagCode_id_md5) != tagCodeId) {

            // 进入知识点总人次 + 1
            tagCodeEntryNumbers += 1


            // 处理上一条知识点数据


            // 累计能力值
            val lastAbility = jedis.get(accountId_course_topic_section_tagCode_last_ability_md5)
            if (lastAbility != null) {
              tagCodeAbilitySum += lastAbility.toDouble
            }

            // 统计知识点掌握情况

            val lastLoStatus = jedis.get(accountId_course_topic_section_tagCode_last_lo_status_md5)
            if (lastLoStatus != null)
              lastLoStatus match {
                case "PASSED" => loStatusPASSED += 1
                case "FAILED" => loStatusFAILED += 1
                case "INPROCESS" => loStatusINPROCESS += 1
                case "DEALED" => loStatusDEALED += 1
                case "NODEALED" => loStatusNODEALED += 1
                case "UNCERTAIN" => loStatusUNCERTAIN += 1
                case "NOITEM" => loStatusNOITEM += 1
              }

            // 统计知识点完成总时间
            val fisrtTime = jedis.get(accountId_course_topic_section_tagCode_id_first_time_md5)
            if (fisrtTime != null) {
              tagCodeCompletionTime += ((timeStamp - fisrtTime.toLong) / 1000).toInt
            }
            // 统计知识点完成人数
            tagCodeCompletionNumbers += 1


            // 更新上一条知识点数据到数据库


            compositedIdParam = ArrayBuffer[Any](dataObj.get("course_id"), dataObj.get("lesson_id"), dataObj.get("topic_id"), dataObj.get("module_type")
              , jedis.get(accountId_course_topic_section_tagCode_id_md5))

            params.clear()
            params ++= ArrayBuffer[Any](tagCodeEntryNumbers, tagCodeAbilitySum, loStatusPASSED, loStatusFAILED, loStatusINPROCESS,
              loStatusDEALED, loStatusNODEALED, loStatusUNCERTAIN, loStatusNOITEM, tagCodeCompletionTime,
              tagCodeCompletionNumbers)
            params ++= compositedIdParam
            val rs_data = C3p0Pools.execute(updateLastTagCodeSql, params.toArray[Any], conn)
            if (rs_data.equals(1)) {
              println("上一条 知识点的数据归档 成功")

            }

            // 上一条数据归档成功后 要处理新的
            // redis 更新缓存
            jedis.set(accountId_course_topic_section_tagCode_id_md5, tagCodeId)
            jedis.set(accountId_course_topic_section_tagCode_id_first_time_md5, timeStampStr)
            jedis.set(accountId_course_topic_section_tagCode_last_ability_md5, ability)
            jedis.set(accountId_course_topic_section_tagCode_last_lo_status_md5, loStatus)
            jedis.set(accountId_course_topic_section_tagCode_last_question_id_md5, questionId)


          } else {
            //知识点相同 更新能力值 知识点掌握情况
            if (jedis.get(accountId_course_topic_section_tagCode_last_question_id_md5) != questionId) {

              jedis.set(accountId_course_topic_section_tagCode_last_lo_status_md5, loStatus)
              jedis.set(accountId_course_topic_section_tagCode_last_ability_md5, ability)
              jedis.set(accountId_course_topic_section_tagCode_last_question_id_md5, questionId)

            }
          }

        }


        // 知识点下做题总用时（秒）
        tagCodeAnswerTime += dataObj.getIntValue("cost_time")

        // 知识点下做题总数
        answerNumbers += 1

        // 知识点下做对题数,知识点下做错题数, 知识点下不会题数

        val isRight = dataObj.get("is_right").toString
        isRight match {
          case "0" => answerWrongNumbers += 1
          case "1" => answerRightNumbers += 1
          case "2" => incompetentNumbers += 1
        }

        // 知识点下看过解析做题总数、做对、做错、不会

        val isViewAnalyze = dataObj.get("is_view_analyze").toString
        isViewAnalyze match {
          case "0" =>
            println("未看过解析")

          // 看过解析统计: 答对，答错，不会的人次
          case "1" =>
            // 看过解析总人数
            viewAnalysisNumbers += 1
            isRight match {
              case "0" => viewWrongNumbers += 1
              case "1" => viewRightNumbers += 1
              case "2" => viewIncompetentNumbers += 1
            }

        }

        // 更新数据库

        params.clear()
        params ++= ArrayBuffer[Any](tagCodeEntryNumbers, tagCodeCompletionNumbers, answerNumbers, answerRightNumbers,
          answerWrongNumbers, incompetentNumbers, viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers
          , viewIncompetentNumbers, tagCodeAnswerTime, tagCodeCompletionTime, tagCodeAbilitySum, loStatusPASSED, loStatusFAILED
          , loStatusINPROCESS, loStatusDEALED, loStatusNODEALED, loStatusUNCERTAIN, loStatusNOITEM)
        params ++= compositedIdParam
        val rs_data = C3p0Pools.execute(updateSql, params.toArray[Any], conn)
        if (rs_data.equals(1)) {
          println("当前知识点归档成功")
        }
      }

      // 如果记录数据库中真不存在 初始化插入
      if (!flag) {
        params.clear()
        params ++= compositedIdParam
        val rsInsert = C3p0Pools.execute(insertSql, params.toArray[Any], conn)
        if (rsInsert.equals(1))
          println("数据库 成功 新增一条知识点数据 ~")

        var (tagCodeEntryNumbers, tagCodeCompletionNumbers, answerNumbers, answerRightNumbers, answerWrongNumbers
        , incompetentNumbers, viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers
        , tagCodeAnswerTime, tagCodeCompletionTime, tagCodeAbilitySum, loStatusPASSED, loStatusFAILED, loStatusINPROCESS
        , loStatusDEALED, loStatusNODEALED, loStatusUNCERTAIN, loStatusNOITEM) = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0.0, 0, 0, 0, 0, 0, 0, 0)


        if (jedis.get(accountId_course_topic_section_tagCode_id_md5) == null) {

          // 进入知识点数量+1
          tagCodeEntryNumbers += 1

          // 写入缓存
          jedis.set(accountId_course_topic_section_tagCode_id_md5, tagCodeId)
          jedis.set(accountId_course_topic_section_tagCode_id_first_time_md5, timeStampStr)
          jedis.set(accountId_course_topic_section_tagCode_last_ability_md5, ability)
          jedis.set(accountId_course_topic_section_tagCode_last_lo_status_md5, loStatus)
          jedis.set(accountId_course_topic_section_tagCode_last_question_id_md5, questionId)

        } else {


          if (jedis.get(accountId_course_topic_section_tagCode_id_md5) != tagCodeId) {


            compositedIdParam = ArrayBuffer[Any](dataObj.get("course_id"), dataObj.get("lesson_id"), dataObj.get("topic_id"), dataObj.get("module_type")
              , jedis.get(accountId_course_topic_section_tagCode_id_md5))

            params.clear()
            params ++= compositedIdParam
            rs = C3p0Pools.query(conn, searchSql, params.toArray[Any])
            while (rs.next()) {

              tagCodeEntryNumbers = rs.getInt("tagCodeEntryNumbers")
              tagCodeCompletionNumbers = rs.getInt("tagCodeCompletionNumbers")
              tagCodeCompletionTime = rs.getInt("tagCodeCompletionTime")
              tagCodeAbilitySum = rs.getDouble("tagCodeAbilitySum")
              loStatusPASSED = rs.getInt("loStatusPASSED")
              loStatusFAILED = rs.getInt("loStatusFAILED")
              loStatusINPROCESS = rs.getInt("loStatusINPROCESS")
              loStatusDEALED = rs.getInt("loStatusDEALED")
              loStatusNODEALED = rs.getInt("loStatusNODEALED")
              loStatusUNCERTAIN = rs.getInt("loStatusUNCERTAIN")
              loStatusNOITEM = rs.getInt("loStatusNOITEM")

              // 进入知识点总人次 + 1
              tagCodeEntryNumbers += 1


              // 处理上一条知识点数据

              // 能力值 累加  取变换知识点前的左后一道题的能力值 ,  同理知识点掌握情况

              /// 累计能力值
              val lastAbility = jedis.get(accountId_course_topic_section_tagCode_last_ability_md5)
              if (lastAbility != null) {
                tagCodeAbilitySum += lastAbility.toDouble
              }

              // 统计知识点掌握情况

              val lastLoStatus = jedis.get(accountId_course_topic_section_tagCode_last_lo_status_md5)
              if (lastLoStatus != null)
                lastLoStatus match {
                  case "PASSED" => loStatusPASSED += 1
                  case "FAILED" => loStatusFAILED += 1
                  case "INPROCESS" => loStatusINPROCESS += 1
                  case "DEALED" => loStatusDEALED += 1
                  case "NODEALED" => loStatusNODEALED += 1
                  case "UNCERTAIN" => loStatusUNCERTAIN += 1
                  case "NOITEM" => loStatusNOITEM += 1
                }

              // 统计知识点完成总时间
              val fisrtTime = jedis.get(accountId_course_topic_section_tagCode_id_first_time_md5)
              if (fisrtTime != null) {
                tagCodeCompletionTime += ((timeStamp - fisrtTime.toLong) / 1000).toInt
              }

              // 统计知识点完成人数
              tagCodeCompletionNumbers += 1


              // 更新上一条知识点数据到数据库
              params.clear()
              params ++= ArrayBuffer[Any](tagCodeEntryNumbers, tagCodeAbilitySum, loStatusPASSED, loStatusFAILED, loStatusINPROCESS,
                loStatusDEALED, loStatusNODEALED, loStatusUNCERTAIN, loStatusNOITEM, tagCodeCompletionTime,
                tagCodeCompletionNumbers)
              params ++= compositedIdParam
              val rs_data = C3p0Pools.execute(updateLastTagCodeSql, params.toArray[Any], conn)
              if (rs_data.equals(1)) {
                println("上一条知识点数据归档成功")
              }


              // redis 更新缓存
              jedis.set(accountId_course_topic_section_tagCode_id_md5, tagCodeId)
              jedis.set(accountId_course_topic_section_tagCode_id_first_time_md5, timeStampStr)

              jedis.set(accountId_course_topic_section_tagCode_last_ability_md5, ability)
              jedis.set(accountId_course_topic_section_tagCode_last_lo_status_md5, loStatus)
              jedis.set(accountId_course_topic_section_tagCode_last_question_id_md5, questionId)

            }
          }
          else {
            //知识点相同 更新能力值 知识点掌握情况
            if (jedis.get(accountId_course_topic_section_tagCode_last_question_id_md5) != questionId) {
              jedis.set(accountId_course_topic_section_tagCode_last_question_id_md5, questionId)
              jedis.set(accountId_course_topic_section_tagCode_last_lo_status_md5, loStatus)
              jedis.set(accountId_course_topic_section_tagCode_last_ability_md5, ability)

            }
          }

        }


        answerNumbers += 1

        tagCodeAnswerTime += dataObj.getIntValue("cost_time")


        val isRight = dataObj.get("is_right").toString
        isRight match {
          case "0" => answerWrongNumbers += 1
          case "1" => answerRightNumbers += 1
          case "2" => incompetentNumbers += 1
        }

        // 知识点下看过解析做题总数、做对、做错、不会

        val isViewAnalyze = dataObj.get("is_view_analyze").toString
        isViewAnalyze match {
          case "0" =>
            println("未看过解析")

          // 看过解析统计: 答对，答错，不会的人次
          case "1" =>
            // 看过解析总人数
            viewAnalysisNumbers += 1
            isRight match {
              case "0" => viewWrongNumbers += 1
              case "1" => viewRightNumbers += 1
              case "2" => viewIncompetentNumbers += 1
            }

        }

        params.clear()
        params ++= ArrayBuffer[Any](tagCodeEntryNumbers, tagCodeCompletionNumbers, answerNumbers, answerRightNumbers
          , answerWrongNumbers, incompetentNumbers, viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers
          , viewIncompetentNumbers, tagCodeAnswerTime, tagCodeCompletionTime, tagCodeAbilitySum, loStatusPASSED
          , loStatusFAILED, loStatusINPROCESS, loStatusDEALED, loStatusNODEALED, loStatusUNCERTAIN, loStatusNOITEM)
        params ++= compositedIdParam
        val rs_data = C3p0Pools.execute(updateSql, params.toArray[Any], conn)
        if (rs_data.equals(1)) {
          println("对数据库 新增的知识点数据 归档成功")
        }

      }
    }

  }

  def handleQuestionNationWide(line: String , conn : Connection , jedis : Jedis): Unit = {
    // 1.1.1 试题

    //初始化

    val params = ArrayBuffer[Any]()


    // 解析每一行数据为Json对象
    val jsonStr = line.substring(line.indexOf('{'))
    val json = JsonUtil.getObjectFromJson(jsonStr)


    // 获取data
    val dataObj = JsonUtil.getObjectFromJson(json.get("data").toString)
    val srcType = dataObj.get("src_type")

    // 表 study_archive_nationwide_questions 用于试题相关的归档
    val searchSql = "select * from study_archive_nationwide_questions where courseId = ? and lessonId = ? and topicId = ?  and sectionId = ? " +
      "and tagCodeId = ? and questionId = ? "
    val insertSql = "insert into study_archive_nationwide_questions (courseId , lessonId, topicId , sectionId, tagCodeId, questionId) values " +
      "(?, ?, ?, ?, ?, ?) "
    val updateSql = " update study_archive_nationwide_questions  set   answerNumbers = ?, answerRightNumbers = ?, " +
      "answerWrongNumbers = ?, incompetentNumbers = ?, viewAnalysisNumbers = ?, viewRightNumbers = ?, " +
      "viewWrongNumbers = ?, viewIncompetentNumbers = ? " +
      " where courseId = ? and lessonId = ? and topicId = ? and sectionId = ? and tagCodeId = ? and questionId = ? "





    // 判断src_type
    if (srcType == "SUBMIT_QUESTION") {

      // 相关sql参数
      var compositedIdParam = ArrayBuffer[Any](dataObj.get("course_id"), dataObj.get("lesson_id"), dataObj.get("topic_id"), dataObj.get("module_type")
        , dataObj.get("tag_code"), dataObj.get("question_id"))


      // 根据i更新统计结果
      params ++= compositedIdParam

      val rs = C3p0Pools.query(conn, searchSql, params.toArray[Any])

      // 默认记录在数据库不存在
      var flag = false

      while (rs.next()) {
        if (!flag) {
          flag = true
        }
        // 从rs中获取当前试题的变量


        var answerNumbers = rs.getInt("answerNumbers")
        var answerRightNumbers = rs.getInt("answerRightNumbers")
        var answerWrongNumbers = rs.getInt("answerWrongNumbers")
        var incompetentNumbers = rs.getInt("incompetentNumbers")
        var viewAnalysisNumbers = rs.getInt("viewAnalysisNumbers")
        var viewRightNumbers = rs.getInt("viewRightNumbers")
        var viewWrongNumbers = rs.getInt("viewWrongNumbers")
        var viewIncompetentNumbers = rs.getInt("viewIncompetentNumbers")


        // 做题总数
        answerNumbers += 1

        // 做对题数, 做错题数, 不会题数

        val isRight = dataObj.get("is_right").toString
        isRight match {
          case "0" => answerWrongNumbers += 1
          case "1" => answerRightNumbers += 1
          case "2" => incompetentNumbers += 1
        }

        // 解析做题总数、做对、做错、不会

        val isViewAnalyze = dataObj.get("is_view_analyze").toString
        isViewAnalyze match {
          case "0" =>
            println("做该题时 未看过解析")

          // 看过解析统计: 答对，答错，不会的人次
          case "1" =>
            // 看过解析总人数
            viewAnalysisNumbers += 1
            isRight match {
              case "0" => viewWrongNumbers += 1
              case "1" => viewRightNumbers += 1
              case "2" => viewIncompetentNumbers += 1
            }

        }

        // 更新数据库

        params.clear()
        params ++= ArrayBuffer[Any](answerNumbers, answerRightNumbers, answerWrongNumbers, incompetentNumbers
          , viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers)
        params ++= compositedIdParam
        val rs_data = C3p0Pools.execute(updateSql, params.toArray[Any], conn)
        if (rs_data.equals(1))
          println("当前试题归档成功")

      }

      // 如果记录数据库中真不存在 初始化插入
      if (!flag) {
        params.clear()
        params ++= compositedIdParam
        val rsInsert = C3p0Pools.execute(insertSql, params.toArray[Any], conn)
        if (rsInsert.equals(1)) {
          println("数据库新增做题数据成功")
        }

        // 对进入的第一条数据 进行数据库更新
        // courseId ,courseEntryNumbers,answerNumbers ,answerRightNumbers ,answerWrongNumbers ,incompetentNumbers
        // ,viewAnalysisNumbers ,viewRightNumbers ,viewWrongNumbers ,viewIncompetentNumbers ,courseAnswerTime

        var (answerNumbers, answerRightNumbers, answerWrongNumbers, incompetentNumbers
        , viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers) = (0, 0, 0, 0, 0, 0, 0, 0)

        answerNumbers += 1

        val isRight = dataObj.get("is_right").toString
        isRight match {
          case "0" => answerWrongNumbers += 1
          case "1" => answerRightNumbers += 1
          case "2" => incompetentNumbers += 1
        }

        // 课程下看过解析做题总数、做对、做错、不会

        val isViewAnalyze = dataObj.get("is_view_analyze").toString
        isViewAnalyze match {
          case "0" =>
            println("未看过解析")

          // 看过解析统计: 答对，答错，不会的人次
          case "1" =>
            // 看过解析总人数
            viewAnalysisNumbers += 1
            isRight match {
              case "0" => viewWrongNumbers += 1
              case "1" => viewRightNumbers += 1
              case "2" => viewIncompetentNumbers += 1
            }

        }

        // 更新数据库

        params.clear()
        params ++= ArrayBuffer[Any](answerNumbers, answerRightNumbers, answerWrongNumbers, incompetentNumbers
          , viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers)
        params ++= compositedIdParam
        val rs_data = C3p0Pools.execute(updateSql, params.toArray[Any], conn)
        if (rs_data.equals(1)) {
          println("当前的做题数据归档成功")
        }

      }
    }

  }

  def handleFactorCodesNationWide(line: String , conn : Connection , jedis : Jedis): Unit = {
    // 1.1.6 能力维度

    //初始化

    val params = ArrayBuffer[Any]()


    // 解析每一行数据为Json对象
    val jsonStr = line.substring(line.indexOf('{'))
    val json = JsonUtil.getObjectFromJson(jsonStr)

    // 获取data
    val dataObj = JsonUtil.getObjectFromJson(json.get("data").toString)
    val srcType = dataObj.get("src_type")

    // 表 study_archive_nationwide_factorCodes 用于知识点相关的归档
    val searchSql = "select * from study_archive_nationwide_factorCodes where courseId = ? " +
      "and lessonId = ? and topicId = ? and sectionId = ? and factorCode = ? "

    val insertSql = "insert into study_archive_nationwide_factorCodes (courseId , lessonId , topicId , sectionId, factorCode) values " +
      "(?, ?, ?, ?, ?) "
    val updateSql = " update study_archive_nationwide_factorCodes  set triggerNumbers = ? , answerNumbers   = ? , " +
      "answerRightNumbers   = ? , answerWrongNumbers   = ? , incompetentNumbers   = ? , viewAnalysisNumbers   = ? " +
      ", viewRightNumbers   = ? , viewWrongNumbers   = ? , viewIncompetentNumbers  = ? , factorCodeAbility = ? " +
      "where courseId = ? and lessonId = ? and topicId = ? and sectionId = ? and factorCode = ?"



    // 判断src_type
    if (srcType == "SUBMIT_QUESTION") {
      // 根据i更新统计结果


      //获取dataObj中的数据
      val accountId = dataObj.get("account_id").toString
      val topicId = dataObj.get("topic_id").toString
      val courseId = dataObj.get("course_id").toString
      val sectionId = dataObj.get("module_type").toString
      val lessonId = dataObj.get("lesson_id").toString


      val factorCodes = dataObj.getJSONArray("factor_codes").toArray
      val arraySize = factorCodes.size

      if (arraySize >= 1) {

        for (i <- 0 until arraySize ) {

          var accountId_course_topic_section_factorCode_id_md5 = Md5Util.md5(accountId + "_" + courseId + "_" + lessonId + "_" + topicId + "_" + sectionId + "_" + factorCodes(i).toString)

          // 相关sql参数
          var compositedIdParam = ArrayBuffer[Any](dataObj.get("course_id"), dataObj.get("lesson_id"), dataObj.get("topic_id"), dataObj.get("module_type")
            , factorCodes(i).toString)

          params.clear()
          params ++= compositedIdParam

          val rs = C3p0Pools.query(conn, searchSql, params.toArray[Any])

          // 默认记录在数据库不存在
          var flag = false

          while (rs.next()) {
            if (!flag) {
              flag = true
            }

            //  triggerNumbers,  answerNumbers , answerRightNumbers , answerWrongNumbers , incompetentNumbers ,
            // viewAnalysisNumbers , viewRightNumbers , viewWrongNumbers , viewIncompetentNumbers, factorCodeAbility

            // 从rs中获取当前factorCode的相关参数
            var triggerNumbers = rs.getInt("triggerNumbers")
            var answerNumbers = rs.getInt("answerNumbers")
            var answerRightNumbers = rs.getInt("answerRightNumbers")
            var answerWrongNumbers = rs.getInt("answerWrongNumbers")
            var incompetentNumbers = rs.getInt("incompetentNumbers")
            var viewAnalysisNumbers = rs.getInt("viewAnalysisNumbers")
            var viewRightNumbers = rs.getInt("viewRightNumbers")
            var viewWrongNumbers = rs.getInt("viewWrongNumbers")
            var viewIncompetentNumbers = rs.getInt("viewIncompetentNumbers")
            var factorCodeAbility = rs.getDouble("factorCodeAbility")


            // 判断  用户是否触发过 能力维度
            if (jedis.get(accountId_course_topic_section_factorCode_id_md5) == null) {
              // 进入知识点总人次 + 1
              triggerNumbers += 1

              // 写入缓存
              jedis.set(accountId_course_topic_section_factorCode_id_md5, factorCodes(i).toString)

            }


            // 能力维度下做题总数
            answerNumbers += 1

            // 能力维度下做对题数,能力维度下做错题数, 能力维度下不会题数

            val isRight = dataObj.get("is_right").toString
            isRight match {
              case "0" => answerWrongNumbers += 1
              case "1" => answerRightNumbers += 1
              case "2" => incompetentNumbers += 1
            }

            // 能力维度下看过解析做题总数、做对、做错、不会

            val isViewAnalyze = dataObj.get("is_view_analyze").toString
            isViewAnalyze match {
              case "0" =>
                println("未看过解析")

              // 看过解析统计: 答对，答错，不会的人次
              case "1" =>
                // 看过解析总人数
                viewAnalysisNumbers += 1
                isRight match {
                  case "0" => viewWrongNumbers += 1
                  case "1" => viewRightNumbers += 1
                  case "2" => viewIncompetentNumbers += 1
                }

            }

            factorCodeAbility = answerRightNumbers.toDouble / answerNumbers.toDouble


            // 更新数据库

            params.clear()
            params ++= ArrayBuffer[Any](triggerNumbers, answerNumbers, answerRightNumbers, answerWrongNumbers, incompetentNumbers,
              viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers, factorCodeAbility)
            params ++= compositedIdParam
            val rs_data = C3p0Pools.execute(updateSql, params.toArray[Any], conn)
            if (rs_data.equals(1)) {
              println("当前能力维度数据 归档成功")
            }
          }

          // 如果记录数据库中真不存在 初始化插入
          if (!flag) {
            params.clear()
            params ++= compositedIdParam
            val rsInsert = C3p0Pools.execute(insertSql, params.toArray[Any], conn)
            if (rsInsert.equals(1))
              println("数据库新增该能力维度的第一条数据成功")

            // ************ 需要对一条数据进行插入


            var (triggerNumbers, answerNumbers, answerRightNumbers, answerWrongNumbers, incompetentNumbers,
            viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers, factorCodeAbility)
            = (0, 0, 0, 0, 0, 0, 0, 0, 0, 0.0)

            triggerNumbers += 1

            answerNumbers += 1


            val isRight = dataObj.get("is_right").toString
            isRight match {
              case "0" => answerWrongNumbers += 1
              case "1" => answerRightNumbers += 1
              case "2" => incompetentNumbers += 1
            }

            // 能力维度下看过解析做题总数、做对、做错、不会

            val isViewAnalyze = dataObj.get("is_view_analyze").toString
            isViewAnalyze match {
              case "0" =>
                println("未看过解析")

              // 看过解析统计: 答对，答错，不会的人次
              case "1" =>
                // 看过解析总人数
                viewAnalysisNumbers += 1
                isRight match {
                  case "0" => viewWrongNumbers += 1
                  case "1" => viewRightNumbers += 1
                  case "2" => viewIncompetentNumbers += 1
                }

            }

            factorCodeAbility = answerRightNumbers / answerNumbers


            params.clear()
            params ++= ArrayBuffer[Any](triggerNumbers, answerNumbers, answerRightNumbers, answerWrongNumbers, incompetentNumbers,
              viewAnalysisNumbers, viewRightNumbers, viewWrongNumbers, viewIncompetentNumbers, factorCodeAbility)
            params ++= compositedIdParam
            val rs_data = C3p0Pools.execute(updateSql, params.toArray[Any], conn)
            if (rs_data.equals(1)) {
              println("当前阶段归档成功")
            }

            // 写入缓存
            jedis.set(accountId_course_topic_section_factorCode_id_md5, factorCodes(i).toString)

          }
        }
      }

    }

  }

}
