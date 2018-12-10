package study.archivedaily

package cn.classba.violet.study.archivedaily

import java.sql.{Connection, PreparedStatement}

import utils.{JsonUtil, _}
import com.alibaba.fastjson.JSONObject
import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer

/**
  * Created by Administrator on 2018/10/22.
  */
class Question {

  /**
    * 测试试题归档
    */
  def archive(conn: Connection, jedis: Jedis, line: String): Unit = {
    val params = ArrayBuffer[Any]()
    //得到每一行的json数据
    val index = line.indexOf('{')
    val jsonStr = line.substring(index)
    val jsonObj: JSONObject = JsonUtil.getObjectFromJson(jsonStr)

    // 时间
    val time = jsonObj.get("time")
    val timeStr = time.toString.substring(0, 10).replace("-", "")
    //解析后的数据
    val data: JSONObject = JsonUtil.getObjectFromJson(jsonObj.get("data").toString)
    val src_type = data.get("src_type")


    //对数据类型记进行判断
    if (src_type.equals("SUBMIT_QUESTION")) {
      // 联合主键：course_id lesson_id topic_id module_type tag_code question_id time
      var idsArr = ArrayBuffer[Any](data.get("course_id"), data.get("lesson_id"), data.get("topic_id"), data.get("module_type"), data.get("tag_code"), data.get("question_id"), timeStr)

      //联合主键
      //id : course_id-lesson_id-topic_id-module_type-tag_code-question_id-time
      // course_id lesson_id topic_id module_type tag_code question_id time
      // 表 study_archive_testquestions_byday 用于试题相关的归档
      val search = "select * from study_archive_daily_testquestions where course_id = ? and lesson_id = ? and topic_id = ? and module_type = ? and tag_code = ? and question_id = ? and time = ?"
      val insert = "replace into study_archive_daily_testquestions(course_id, lesson_id, topic_id, module_type, tag_code, question_id, time, answer, answerRight, answerWrong, incapable, sawAnalysis, sawRight, sawWrong, sawUnable) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
      val update = "update study_archive_daily_testquestions set answer = ?, answerRight = ?, answerWrong = ?, incapable = ?, sawAnalysis = ?, sawRight = ?, sawWrong = ?, sawUnable = ? where course_id = ? and lesson_id = ? and topic_id = ? and module_type = ? and tag_code = ? and question_id = ? and time = ?"

      // 根据data中的数据查表
      // 根据id跟新统计结果
      params.clear()
      params ++= idsArr
      val rs = C3p0Pools.query(conn, search, params.toArray[Any])

      // 当前id的归档数据已存在
      var flag = false
      while (rs.next()) {
        if (!flag) flag = true

        var answer = rs.getInt("answer") + 1 //答题总人次+1
        var answerRight = rs.getInt("answerRight")
        var answerWrong = rs.getInt("answerWrong")
        var incapable = rs.getInt("incapable")
        var sawAnalysis = rs.getInt("sawAnalysis")
        var sawRight = rs.getInt("sawRight")
        var sawWrong = rs.getInt("sawWrong")
        var sawUnable = rs.getInt("sawUnable")

        /**
          * 统计：答题总人次，答对总人次，打错总人次
          * is_right 答案正误 1为正确 0为错误 2为不会
          */

        val test1 = data.get("is_right")
        val is_right_Data = data.get("is_right").toString
        is_right_Data match {
          case "0" => answerWrong += 1
          case "1" => answerRight += 1
          case "2" => incapable += 1
        }

        /**
          * 统计：看过解析总人次, 0表示没看过，1表示看过
          */
        val is_view_analyze_Data = data.get("is_view_analyze").toString
        if(is_view_analyze_Data.equals("1"))sawAnalysis += 1
        is_view_analyze_Data match {
          case "0" => {
          }

          /**
            * 看过解析统计: 答对，答错，不会的人次
            */
          case "1" => {
            is_right_Data match {
              case "0" => sawWrong += 1
              case "1" => sawRight += 1
              case "2" => sawUnable += 1
            }
          }
        }
        //跟新已有归档数据
        var psm_update: PreparedStatement = conn.prepareStatement(update)
        params.clear()
        params ++= Array(answer, answerRight, answerWrong, incapable, sawAnalysis, sawRight, sawWrong, sawUnable)
        params ++= idsArr

        //跟新表中的归档数据
        val rs_update = C3p0Pools.execute(update, params.toArray[Any], conn)
        if (rs_update.equals(1)) println("试题归档成功")
      }

      // rs的值为空，该id的试题归档数据不存在
      if (!flag) {
        params.clear()
        params ++= idsArr
        params ++= Array(1, 0, 0, 0, 0, 0, 0, 0)
        /**
          * 统计：答题总人次，答对总人次，打错总人次
          * is_right 答案正误 1为正确 0为错误 2为不会
          */

        val test1 = data.get("is_right")
        val is_right_Data = data.get("is_right").toString
        is_right_Data match {
          case "0" => params.update(9, 1)
          case "1" => params.update(8, 1)
          case "2" => params.update(10, 1)
        }

        /**
          * 统计：看过解析总人次, 0表示没看过，1表示看过
          */
        val is_view_analyze_Data = data.get("is_view_analyze").toString
        if(is_view_analyze_Data.equals("1"))params.update(11, 1)
        is_view_analyze_Data match {
          case "0" => {
          }

          /**
            * 看过解析统计: 答对，答错，不会的人次
            */
          case "1" => {
            is_right_Data match {
              case "0" => params.update(13, 1)
              case "1" => params.update(12, 1)
              case "2" => params.update(14, 1)
            }
          }
        }
        //先初始化归档数据
        val rs_update = C3p0Pools.execute(insert, params.toArray[Any], conn)
        if (rs_update.equals(1)) println("试题归档成功")
      }
    }
    println("该条数据试题归档结束")
  }

}

