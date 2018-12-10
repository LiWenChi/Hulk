package study.archivedaily

/**
  * Created by LiWenChi on 2018/10/18.
  * 能力维度归档
  */

import java.sql.Connection

import utils._
import com.alibaba.fastjson.JSONObject
import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer

class Factor {
  // course_id lesson_id topic_id stage factor_codes time
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

    //对数据类型记进行判断
    if (src_type.equals("SUBMIT_QUESTION")) {

      //联合主键
      var factorArr = ArrayBuffer[Any]()
      factorArr ++= data.getJSONArray("factor_codes").toArray.toBuffer
      //    course_id lesson_id topic_id stage factor_codes time
      var idsArr = ArrayBuffer[Any]()

      // 表 study_archive_daily_factor 用于试题相关的归档
      //联合主键： course_id lesson_id topic_id stage factor_codes time
      val search = "select * from study_archive_daily_factor where course_id = ? and lesson_id = ? and topic_id = ? and stage = ? and factor_codes = ? and time = ?"
      val insert = "replace into study_archive_daily_factor(" +
        "course_id, lesson_id, topic_id, stage, factor_codes, time, factor, answer, answerRight, answerWrong, incapable, sawAnalysis, sawRight, sawWrong, sawUnable, value) " +
        "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
      //        "ON DUPLICATE KEY UPDATE "
      val update = "update study_archive_daily_factor  set factor = ?, answer = ?, answerRight = ?, answerWrong = ?, incapable = ?, sawAnalysis = ?, sawRight = ?, sawWrong = ?, sawUnable = ?, value = ? where course_id = ? and lesson_id = ? and topic_id = ? and stage = ? and factor_codes = ? and time = ?"

      //当前日志的用户和能力值维度数组
      var account_id = data.get("account_id").toString
      //能力值数组中第i个能力纬度值
      var account_id_factor_code = ""
      var factor_present = ""
      // 根据i更新统计结果
      for (j <- factorArr.indices) {
        params.clear()
        idsArr.clear()
        factor_present = factorArr(j).toString
        idsArr ++= ArrayBuffer[Any](data.get("course_id"), data.get("lesson_id"), data.get("topic_id"), data.get("stage"))
        idsArr += factor_present
        idsArr += timeStr

        params ++= idsArr

        //当前日志的用户和能力值维度数组
        var account_id = data.get("account_id").toString
        //能力值数组中第i个能力纬度值
        account_id_factor_code = timeStr + account_id + factor_present

        val rs = C3p0Pools.query(conn, search, params.toArray[Any])

        // 当前id的归档数据在表中已存在
        var flag = false
        //表中存在该能力值的归档数据
        while (rs.next()) {
          if (!flag) flag = true
          var factor = rs.getInt("factor") //触发能力维度总人数
          var answer = rs.getInt("answer")
          var answerRight = rs.getInt("answerRight")
          var answerWrong = rs.getInt("answerWrong")
          var incapable = rs.getInt("incapable") //不会做题目数
          var sawAnalysis = rs.getInt("sawAnalysis") //看过解析做题总数
          var sawRight = rs.getInt("sawRight")
          var sawWrong = rs.getInt("sawWrong")
          var sawUnable = rs.getInt("sawUnable")
          //计算方法：能力维度下做对题次数/能力维度下做题总次数
          var value = rs.getDouble("value") //能力纬度值


          /**
            * 统计触发第I个能力维度值的人次
            * 统计完成知识点人次--course
            */
          //用户首次触发当前能力纬度值
          if (jedis.get(account_id + "factor_code") == null) {
            //触发能力维度人次+1
            factor += 1
            //缓存data中的用户能力纬度值
            jedis.set(account_id + "factor_code", factor_present)
          }

          /**
            * 能力值下做题总数
            */
          answer += 1

          /**
            * 能力值下做对题数, 做错题数, 不会题数
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
          if (is_view_analyze_Data.equals("1")) sawAnalysis += 1
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
            * 计算能力值--value
            */
          value = answerRight.toDouble / answer.toDouble

          //对当前id的数据进行跟新
          params.clear()
          params ++= ArrayBuffer[Any](factor, answer, answerRight, answerWrong, incapable, sawAnalysis, sawRight, sawWrong, sawUnable, value)
          params ++= idsArr
          println("==========================================148执行跟新操作===========================================")
          val rs_data = C3p0Pools.execute(update, params.toArray[Any], conn)
          if (rs_data.equals(1)) println("当前id的能力值归档成功")
        }

        // rs的值为空，该id的试题归档数据不存在
        if (!flag) {
          params.clear()
          idsArr.clear()
          idsArr ++= ArrayBuffer[Any](data.get("course_id"), data.get("lesson_id"), data.get("topic_id"), data.get("stage"))
          factor_present = factorArr(j).toString
          idsArr += factor_present
          idsArr += timeStr
          params ++= idsArr
          //用户首次触发当前能力纬度值
          //触发能力维度人次+1
          params ++= ArrayBuffer[Any](1, 1, 0, 0, 0, 0, 0, 0, 0, 0)
          //缓存data中的用户能力纬度值
          jedis.set(account_id + "factor_code", factor_present)

          /**
            * 知识点下做对题数, 知识点下做错题数, 知识点下不会题数
            */
          val is_right_Data = data.get("is_right").toString
          is_right_Data match {
            case "0" => params.update(9, 1) // answerWrong
            case "1" => params.update(8, 1) // answerRight
            case "2" => params.update(10, 1) // incapable
          }

          /**
            * 知识点下看过解析做题总数、做对、做错、不会
            */
          val is_view_analyze_Data = data.get("is_view_analyze").toString
          if (is_view_analyze_Data.equals("1")) params.update(11, 1) //sawAnalysis
          is_view_analyze_Data match {
            case "0" => {
            }
            // 看过解析统计: 答对，答错，不会的人次
            case "1" => {
              is_right_Data match {
                case "0" => params.update(13, 1) // sawWrong
                case "1" => params.update(12, 1) // sawRight
                case "2" => params.update(14, 1) // sawUnable
              }
            }
          }

          /**
            * 计算能力值
            */
          val value_ = params(8).toString.toInt / 1
          params.update(15, value_)
          println("========================================200行执行插入==============================================")
          val rs_insert = C3p0Pools.execute(insert, params.toArray[Any], conn)
          if (rs_insert.equals(1)) println("能力值归档成功")
        }
      }
    }
    println("该条数据能力值归档结束")
  }
}

