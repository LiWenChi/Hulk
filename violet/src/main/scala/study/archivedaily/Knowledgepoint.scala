package study.archivedaily

/**
  * Created by LiWenChi on 2018/10/18.
  * 知识点归档测试
  */


import java.sql.Connection

import utils._
import com.alibaba.fastjson.JSONObject
import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer

class Knowledgepoint {
  def archive(conn: Connection, jedis: Jedis, line: String): Unit = {
    //    jedis.flushDB()
    val params = ArrayBuffer[Any]()
    //得到每一行的json数据
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
    var idsArr = ArrayBuffer[Any](data.get("course_id"), data.get("lesson_id"), data.get("topic_id"), data.get("module_type"), data.get("tag_code"), timeStr)

    var account_id = data.get("account_id").toString
    var tag_code = data.get("tag_code").toString
    var account_id_tag_code = timeStr + account_id + data.get("tag_code").toString
    //开始和结束时间在redis的缓存的时间都是时间戳

    //对数据类型记进行判断
    if (src_type.equals("SUBMIT_QUESTION")) {

      // 表 study_archive_testquestions_byday 用于试题相关的归档
      //lostatus : PASSED, FAILED, INPROCESS, DEALED, NODEALED, UNCERTAIN, NOITEM
      val search = "select * from study_archive_daily_knowledgepoint where course_id = ? and lesson_id = ? and topic_id = ? and module_type = ? and tag_code = ? and time = ?"
      val insert = "replace into study_archive_daily_knowledgepoint(" +
        "course_id, lesson_id, topic_id, module_type, tag_code, time, kp, kpdone, answer, answerRight, answerWrong, incapable, sawAnalysis, sawRight, sawWrong, sawUnable, doneThemesTime, doneKpTime, abilityValueSum, PASSED, FAILED, INPROCESS, DEALED, NODEALED, UNCERTAIN, NOITEM) " +
        "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
      val update = "update study_archive_daily_knowledgepoint  set kp = ?, kpdone = ?, answer = ?, answerRight = ?, answerWrong = ?, incapable = ?, sawAnalysis = ?, sawRight = ?, sawWrong = ?, sawUnable = ?, doneThemesTime = ?, doneKpTime = ?, abilityValueSum = ?, PASSED = ?, FAILED = ?, INPROCESS = ?, DEALED = ?, NODEALED = ?, UNCERTAIN = ?, NOITEM = ? where course_id = ? and lesson_id = ? and topic_id = ? and module_type = ? and tag_code = ? and time = ?"

      // 根据id更新统计结果
      params ++= idsArr

      val rs = C3p0Pools.query(conn, search, params.toArray[Any])

      // 当前id的归档数据已存在
      var flag = false
      while (rs.next()) {
        if (!flag) flag = true
        //        var id = rs.getString("id")
        var kp = rs.getInt("kp")
        var kpdone = rs.getInt("kpdone")
        var answer = rs.getInt("answer")
        var answerRight = rs.getInt("answerRight")
        var answerWrong = rs.getInt("answerWrong")
        var incapable = rs.getInt("incapable")
        var sawAnalysis = rs.getInt("sawAnalysis")
        var sawRight = rs.getInt("sawRight")
        var sawWrong = rs.getInt("sawWrong")
        var sawUnable = rs.getInt("sawUnable")
        var doneThemesTime = rs.getInt("doneThemesTime")
        var doneKpTime = rs.getInt("doneKpTime")
        var abilityValueSum = rs.getDouble("abilityValueSum")
        //能力值字段
        //lostatus : PASSED, FAILED, INPROCESS, DEALED, NODEALED, UNCERTAIN, NOITEM
        var PASSED = rs.getString("PASSED")
        var FAILED = rs.getString("FAILED")
        var INPROCESS = rs.getString("INPROCESS")
        var DEALED = rs.getString("DEALED")
        var NODEALED = rs.getString("NODEALED")
        var UNCERTAIN = rs.getString("UNCERTAIN")
        var NOITEM = rs.getString("NOITEM")

        //        //缓存data中的id
        //        var md5_id_data = Md5Util.md5(id_data)
        //        jedis.set(md5_id_data, data.get("tag_code").toString)
        //当前日志的用户和知识点

        /**
          * 统计进入知识点人次--kp
          * 统计完成知识点人次--kpdone
          */
        //用户首次开始学习知识点
        if (jedis.get(account_id+"tag_code") == null) {
          //进入知识点人次+1
          kp += 1
          //缓存data中的用户知识点
          jedis.set(account_id+"tag_code", tag_code)

          //缓存用户_知识点_知识点开始时间
          val timeSt_str = timeSt.toString //时间戳的字符串
          jedis.set(account_id+"tag_code_starttime", timeSt_str)

          //缓存知识点结束时间
          var endtime = timeSt.toString
          jedis.set(account_id+"tag_code_endtime", endtime)

          //缓存用户_知识点_题目
          jedis.set(account_id+"tag_code_questions", data.get("question_id").toString)

          //缓存用户_知识点_题目_能力值
          jedis.set(account_id+"tag_code_questions_ability", data.get("ability").toString)

          //缓存用户_知识点_题目_掌握状态
          val status = data.get("lo_status").toString
          jedis.set(account_id+"tag_code_questions_status", status)
        } else {
          //用户非首次学习
          val account_id_tag_code_cache = jedis.get(account_id+"tag_code")
          val account_id_tag_code_data = data.get("tag_code")

          //用户学习的知识点没变，只是做题改变
          if (account_id_tag_code_cache.equals(account_id_tag_code_data)) {
            // 缓存当前题目的能力值
            jedis.set(account_id+"tag_code_questions_ability", data.get("ability").toString)

            //缓存当前知识点做题的结束时间
            val endtime = timeSt.toString
            jedis.set(account_id+"tag_code_endtime", endtime)

            //缓存当前知识点的掌握情况,知识点的掌握状态分为：PASSED//FAILED//INPROCESS//DEALED//NODEALED//UNCERTAIN//NOITEM
            jedis.set(account_id+"tag_code_questions_status", data.get("lo_status").toString)
          }

          //用户的知识点改变，则用户的上一个知识点完成
          //则表示上一个知识点完成，即缓存中的知识点完成
          if (!account_id_tag_code_cache.equals(account_id_tag_code_data)) {
            //对上一个知识点进行统计
            val tag_code_cache = account_id_tag_code_cache //user_id
            //得到联合主键
            val idsArr_last = idsArr
            idsArr_last.update(4, tag_code_cache)
            params.clear()

            params ++= idsArr_last
            val rs_last = C3p0Pools.query(conn, search, params.toArray[Any])
            while (rs_last.next()) {
              var kpdone_last = rs.getInt("kpdone")
              var doneKpTime_last = rs.getInt("doneKpTime")
              var abilityValueSum_last = rs.getDouble("abilityValueSum")
              //              var lostatus_last = rs.getString("lostatus")

              /**
                * 归档：知识点的完成人次+1
                */
              kpdone_last += 1

              val starttime = jedis.get(account_id+"tag_code_starttime").toLong
              val endtime = jedis.get(account_id+"tag_code_endtime").toLong

              /**
                * 归档：知识点的完成时间
                */
              val spendtime = endtime - starttime
              doneKpTime_last = (spendtime / 1000).toInt

              /**
                * 归档：能力值总值
                */
              val ability = jedis.get(account_id+"tag_code_questions_ability")
              abilityValueSum_last += ability.toString.toDouble

              /**
                * 归档：知识点掌握情况
                */
              //得到当前题目的掌握状况
              val status_last = jedis.get(account_id+"tag_code_questions_status")
              var statusValue = rs.getInt(status_last) //从数据库获得当前掌握状况的人数
              statusValue += 1

              //跟新用户上一个已经完成的知识点归档数据
              val update_last = "update study_archive_daily_knowledgepoint set kpdone = ?, doneKpTime = ?, abilityValueSum = ?, "+ status_last + " = ? where course_id=? and lesson_id=? and topic_id=? and module_type=? and tag_code=? and time=?"

              params.clear()
              params += kpdone_last
              params += doneKpTime_last
              params += abilityValueSum_last
              params += statusValue
              params ++= idsArr

              val rs_update_last = C3p0Pools.execute(update_last, params.toArray[Any], conn)
              if (rs_update_last.equals(1)) println("完成上一个知识点的统计")
            }

            //跟新用户知识点缓存
            account_id_tag_code = timeStr + data.get("account_id").toString + data.get("tag_code").toString
            jedis.set(account_id+"tag_code", tag_code)

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
        if(is_view_analyze_Data.equals("1"))sawAnalysis += 1
        is_view_analyze_Data match {
          case "0" => {
            println("未看过解析")
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
        //"id, kp, kpdone, answer, answerRight, answerWrong, incapable, sawAnalysis, sawRight, sawWrong, sawUnable, doneThemesTime, doneKpTime, abilityValueSum, lostatus)
        //        val update = "update study_archive_daily_knowledgepoint  set kp = ?, kpdone = ?, answer = ?, answerRight = ?, answerWrong = ?, incapable = ?, sawAnalysis = ?, sawRight = ?, sawWrong = ?, sawUnable = ?, doneThemesTime = ?, doneKpTime = ?, abilityValueSum = ?, PASSED = ?, FAILED = ?, INPROCESS = ?, DEALED = ?, NODEALED = ?, UNCERTAIN = ?, NOITEM = ? where course_id = ? and lesson_id = ? and topic_id = ? and module_type = ? and tag_code = ? and time = ?"
        params.clear()
        params ++= ArrayBuffer[Any](kp, kpdone, answer, answerRight, answerWrong, incapable, sawAnalysis, sawRight, sawWrong, sawUnable, doneThemesTime, doneKpTime, abilityValueSum, PASSED, FAILED, INPROCESS, DEALED, NODEALED, UNCERTAIN, NOITEM)
        params ++= idsArr
        val rs_data = C3p0Pools.execute(update, params.toArray[Any], conn)
        if (rs_data.equals(1)) println("当前id的知识点归档成功")
      }

      // rs的值为空，该id的试题归档数据不存在
      // 在表中没有该知识点的归档数据，则没有任何用户做过该知识点，则用户的知识点改变，说明上一个知识点完成
      if (!flag) {

        /**
          * 归档用户已经完成的上一个知识点
          */
        var account_id_tag_code_cache = jedis.get(account_id+"tag_code")

        val tag_code_cache = account_id_tag_code_cache //user_id
        if(tag_code_cache != null){
          //得到联合主键
          var idsArr_last = idsArr
          idsArr_last.update(4, tag_code_cache)
          params.clear()

          params ++= idsArr_last
          val rs_last = C3p0Pools.query(conn, search, params.toArray[Any])
          while (rs_last.next()) {
            var kpdone_last = rs_last.getInt("kpdone")
            var doneKpTime_last = rs_last.getInt("doneKpTime")
            var abilityValueSum_last = rs_last.getDouble("abilityValueSum")
            //              var lostatus_last = rs.getString("lostatus")

            /**
              * 归档：知识点的完成人次+1
              */
            kpdone_last += 1

            val starttime = jedis.get(account_id+"tag_code_starttime").toLong
            val endtime = jedis.get(account_id+"tag_code_endtime").toLong

            /**
              * 归档：知识点的完成时间
              */
            val spendtime = endtime - starttime
            doneKpTime_last = (spendtime / 1000.0).toInt

            /**
              * 归档：能力值总值
              */
            val ability = jedis.get(account_id+"tag_code_questions_ability")
            abilityValueSum_last += ability.toString.toDouble

            /**
              * 归档：知识点掌握情况
              */
            //              val statusListMap: mutable.Map[String, Int] = mutable.Map()
            val status_last = jedis.get(account_id+"tag_code_questions_status")
            var statusValue = rs_last.getInt(status_last)
            statusValue += 1

            //跟新用户上一个已经完成的知识点归档数据
            val update_last = "update study_archive_daily_knowledgepoint set kpdone = ?, doneKpTime = ?, abilityValueSum = ?," + status_last + " = ? " + "where course_id=? and lesson_id=? and topic_id=? and module_type=? and tag_code=? and time=?"
            params.clear()
            params += kpdone_last
            params += doneKpTime_last
            params += abilityValueSum_last
            params += statusValue
            params ++= idsArr

            val rs_update_last = C3p0Pools.execute(update_last, params.toArray[Any], conn)
            if (rs_update_last.equals(1)) println("完成上一个知识点的统计")
          }
        }
        //================上一个已经结束的知识点更归档完成==========================

        params.clear()
        params ++= idsArr
        params ++= ArrayBuffer[Any](1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

        /**
          * 知识点下做对题数, 知识点下做错题数, 知识点下不会题数
          */
        val is_right_Data = data.get("is_right").toString
        is_right_Data match {
          case "0" => params.update(10, 1) // answerWrong
          case "1" => params.update(9, 1) // answerRight
          case "2" => params.update(11, 1) // incapable
        }

        /**
          * 知识点下看过解析做题总数、做对、做错、不会
          */
        val is_view_analyze_Data = data.get("is_view_analyze").toString
        if(is_view_analyze_Data.equals("1"))params.update(12, 1) // sawAnalysis
        is_view_analyze_Data match {
          case "0" => {
            println("未看过解析")
          }
          // 看过解析统计: 答对，答错，不会的人次
          case "1" => {
            is_right_Data match {
              case "0" => params.update(14, 1) // sawWrong
              case "1" => params.update(13, 1) // sawRight
              case "2" => params.update(15, 1) // sawUnable
            }
          }
        }

        //归档知识点下做题时间
        params.update(16, data.get("cost_time"))

        //缓存相关数据
        var tag_Code = data.get("tag_code").toString
        //开始和结束时间在redis的缓存的时间都是时间戳

        params.update(4, tag_Code)
        //缓存data中的用户知识点
        val s1 = tag_Code
        val s2 = jedis.set(account_id+"tag_code", tag_Code)
        //缓存用户_知识点_知识点开始时间
        val timeSt_str = timeSt.toString //时间戳的字符串
        jedis.set(account_id+"tag_code_starttime", timeSt_str)

        //缓存知识点结束时间
        val endtime = timeSt.toString
        jedis.set(account_id+"tag_code_endtime", endtime)

        //缓存用户_知识点_题目
        jedis.set(account_id+"tag_code_questions", data.get("question_id").toString)

        //缓存用户_知识点_题目_能力值
        jedis.set(account_id+"tag_code_questions_ability", data.get("ability").toString)

        //缓存用户_知识点_题目_掌握状态
        val status = data.get("lo_status").toString
        jedis.set(account_id+"tag_code_questions_status", status)

        val rs_insert = C3p0Pools.execute(insert, params.toArray[Any], conn)
        if (rs_insert.equals(1)) println("试题归档成功")
      }

    }
    println("该条数据课程归档结束")
  }
}
