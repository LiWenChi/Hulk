package study.archivedaily

/**
  * Created by LiWenChi on 2018/10/18.
  * 课程归档测试
  */

import java.sql.Connection

import utils._
import com.alibaba.fastjson.JSONObject
import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer

class Course {
  // course_id time course answer answerRight answerWrong incapable sawRight sawWrong sawUnable doneThemesTime
  def archive(conn: Connection, jedis: Jedis, line: String): Unit = {
    //    val conn = C3p0Pools.getConnection()
    //    val jedis = JedisUtil.getJedis(1)
    val params = ArrayBuffer[Any]()
    //得到每一行的json数据
    //    val line = "14:33:29.726150 10.10.10.56 00962 {\"time\":\"2018-10-12 14:33:29\",\"exec_time\":0.00016,\"api\":\"\",\"referer\":null,\"client_ip\":null,\"in\":[],\"out\":[],\"extra\":[],\"errorno\":0,\"data\":{\"lo_status\":\"FAILED\",\"factor_codes\":[\"gsn001\",\"gsn009\"],\"user_exam_detail_id\":2215343182192681,\"account_id\":6840100811627941,\"account_id\":8840100811627940,\"subject_id\":2,\"course_id\":1021578998923850,\"lesson_id\":1121578998924114,\"topic_id\":1121578998924115,\"study_card_id\":1121578998924115,\"batch_num\":1,\"course_id\":\"g01020101\",\"tag_name\":\"空集的概念\",\"module_type\":1121578998924188,\"stage\":1,\"audio_analysis\":\"\",\"q_forms\":2,\"q_title_type\":0,\"q_type\":1,\"used_type\":1,\"q_title_id\":\"\",\"question_id\":\"095be0e5bd8111e89bfe506b4bbd5eae\",\"difficulty\":5,\"level\":0,\"estimates_time\":40,\"cost_time\":5,\"right_answer\":\"D\",\"user_answer\":\"B\",\"is_view_answer\":0,\"is_view_analyze\":0,\"is_right\":0,\"is_multi_space_right\":\"[\\\"NORMAL\\\",\\\"ERROR\\\",\\\"NORMAL\\\",\\\"STUDENT_NOT_SELECTED\\\"]\",\"ability\":0.35,\"textbook\":22,\"src_type\":\"SUBMIT_QUESTION\"}}"
    val index = line.indexOf('{')
    val jsonStr = line.substring(index)
    val jsonObj: JSONObject = JsonUtil.getObjectFromJson(jsonStr)

    // 时间
    val time = jsonObj.get("time").toString //2018-10-12 14:36:48
    val timeStr = time.substring(0, 10).replace("-", "") //20181012
    val timeSt: Long = DateUtil.str2ts(time) //时间戳
    //解析后的数据
    val data: JSONObject = JsonUtil.getObjectFromJson(jsonObj.get("data").toString)
    val src_type = data.get("src_type")

    //联合主键
    var idsArr = ArrayBuffer[Any](data.get("course_id"), timeStr)
    //对数据类型记进行判断
    if (src_type.equals("SUBMIT_QUESTION")) {

      // 表 study_archive_daily_course 用于试题相关的归档
      // course_id time course answer answerRight answerWrong incapable sawRight sawWrong sawUnable doneThemesTime
      val search = "select * from study_archive_daily_course where course_id = ? and time = ?"
      val insert = "replace into study_archive_daily_course(" +
        "course_id, time, course, answer, answerRight, answerWrong, incapable, sawRight, sawWrong, sawUnable, doneThemesTime) " +
        "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
      val update = "update study_archive_daily_course  set course = ?, answer = ?, answerRight = ?, answerWrong = ?, incapable = ?, sawRight = ?, sawWrong = ?, sawUnable = ?, doneThemesTime = ? where course_id = ? and time = ?"
      // 根据i更新统计结果
      params ++= idsArr

      val rs = C3p0Pools.query(conn, search, params.toArray[Any])

      // 当前id的归档数据已存在
      var flag = false
      while (rs.next()) {
        if (!flag) flag = true
        var course = rs.getInt("course")
        var answer = rs.getInt("answer")
        var answerRight = rs.getInt("answerRight")
        var answerWrong = rs.getInt("answerWrong")
        var incapable = rs.getInt("incapable")
        var sawRight = rs.getInt("sawRight")
        var sawWrong = rs.getInt("sawWrong")
        var sawUnable = rs.getInt("sawUnable")
        var doneThemesTime = rs.getInt("doneThemesTime")

        //当前日志的用户和知识点
        var account_id = data.get("account_id").toString
        var course_id = data.get("course_id").toString
        var account_id_course_id = timeStr + account_id + data.get("course_id").toString
        //开始和结束时间在redis的缓存的时间都是时间戳
        var account_id_course_id_starttime = ""
        var account_id_course_id_endtime = ""
        var endtime = ""

        /**
          * 统计进入课程人次--
          * 统计完成知识点人次--course
          */
        //用户首次开始学习知识点
        if (jedis.get(account_id+"course_id") == null) {
          //进入课程人次+1
          course += 1
          //缓存data中的用户课程
          jedis.set(account_id+"course_id", course_id)

          //缓存用户_课程开始时间
          val timeSt_str = timeSt.toString //时间戳的字符串
          jedis.set(account_id+"course_id_starttime", timeSt_str)

          //缓存课程结束时间
          endtime = timeSt + (data.get("cost_time").toString.toLong * 1000).toString
          jedis.set(account_id+"course_id_endtime", endtime)

        } else {
          //用户非首次学习该课程
          val account_id_course_id_cache = jedis.get(account_id+"course_id")
          val account_id_course_id_data = data.get("course_id")

          //用户学习的课程没变，只是做题改变
          if (account_id_course_id_cache.equals(account_id_course_id_data)) {

            //缓存当前课程做题的结束时间
            jedis.set(account_id+"course_id_endtime", timeSt.toString)
          }

          //用户的课程改变，则用户的上一个课程完成
          //则表示上一个课程完成，即缓存中的课程完成
          if (!account_id_course_id_cache.equals(account_id_course_id_data)) {

            //跟新用户课程缓存
            jedis.set(account_id+"course_id", course_id)

          }
        }


        /**
          * 知识点下做题总数
          */
        answer += 1

        /**
          * 知识点下做对题数, 知识点下做错题数, 知识点下不会题数
          */
        val is_right_Data = data.get("is_right").toString
        is_right_Data match {
          case "0" => answerWrong += 1
          case "1" => answerRight += 1
          case "2" => incapable += 1
        }

        /**
          * 知识点下看过解析做题总数、做对、做错、不会
          */
        val is_view_analyze_Data = data.get("is_view_analyze").toString
        is_view_analyze_Data match {
          case "0" => {
          }
          // 看过解析统计: 答对，答错，不会的人次
          case "1" => {
            is_right_Data match {
              case "0" => sawWrong += 1
              case "1" => sawRight += 1
              case "2" => sawUnable += 1
            }
          }
        }

        /**
          * 知识点下做题总用时--doneThemesTime
          */
        doneThemesTime += data.get("cost_time").toString.toInt

        //对当前id的数据进行跟新
        params.clear()
        params ++= ArrayBuffer[Any](course, answer, answerRight, answerWrong, incapable, sawRight, sawWrong, sawUnable, doneThemesTime)
        params ++= idsArr
        val rs_data = C3p0Pools.execute(update, params.toArray[Any], conn)
        if (rs_data.equals(1)) println("当前id的课程归档成功")
      }



      // rs的值为空，该id的试题归档数据不存在
      if (!flag) {
        params.clear()
        params ++= idsArr
        params ++= ArrayBuffer[Any](1, 1, 0, 0, 0, 0, 0, 0, 0)

        /**
          * 知识点下做对题数, 知识点下做错题数, 知识点下不会题数
          */
        val is_right_Data = data.get("is_right").toString
        is_right_Data match {
          case "0" => params.update(5, 1) // answerWrong
          case "1" => params.update(4, 1) // answerRight
          case "2" => params.update(6, 1) // incapable
        }

        /**
          * 知识点下看过解析做题总数、做对、做错、不会
          */
        val is_view_analyze_Data = data.get("is_view_analyze").toString
        is_view_analyze_Data match {
          case "0" => {
            println("未看过解析")
          }
          // 看过解析统计: 答对，答错，不会的人次
          case "1" => {
            is_right_Data match {
              case "0" => params.update(8, 1) // sawWrong
              case "1" => params.update(7, 1) // sawRight
              case "2" => params.update(9, 1) // sawUnable
            }
          }
        }

        //归档做题时间
        params.update(10, data.get("cost_time").toString.toInt)

        //缓存相关数据
        var account_id = data.get("account_id").toString
        var course_id = data.get("course_id").toString
        var account_id_course_id = timeStr + account_id + data.get("course_id").toString
        //开始和结束时间在redis的缓存的时间都是时间戳

        //缓存data中的用户知识点
        jedis.set(account_id+"course_id", course_id)
        //缓存用户_知识点_知识点开始时间
        val timeSt_str = timeSt.toString //时间戳的字符串
        jedis.set(account_id+"course_id_starttime", timeSt_str)
        jedis.set(account_id+"course_id_endtime", timeSt_str)

        val rs_insert = C3p0Pools.execute(insert, params.toArray[Any], conn)
        if (rs_insert.equals(1)) println("课程归档成功")
      }
    }
    println("该条数据课程归档结束")
  }
}
