package study.archivedaily


/**
  * Created by LiWenChi on 2018/10/18.
  * section阶段归档
  */

import java.sql.Connection

import utils._
import com.alibaba.fastjson.JSONObject
import redis.clients.jedis.Jedis

import scala.collection.mutable.ArrayBuffer

class Sectiontype {

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


    // 表 study_archive_daily_section 用于试题相关的归档
    val search = "select * from study_archive_daily_section where course_id = ? and topic_id = ? and section_id = ? and time = ?"
    val insert = "replace into study_archive_daily_section(" +
      "course_id, topic_id, section_id, time, section, sectionDone, answer, answerRight, answerWrong, incapable, sawAnalysis, sawRight, sawWrong, sawUnable, doneThemesTime, doneSectionTime) " +
      "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    val update = "update study_archive_daily_section set section = ?, sectionDone = ?, answer = ?, answerRight = ?, answerWrong = ?, incapable = ?, sawAnalysis = ?, sawRight = ?, sawWrong = ?, sawUnable = ?, doneThemesTime = ?, doneSectionTime = ? where course_id = ? and topic_id = ? and section_id = ? and time = ?"

    var account_id = data.get("account_id")
    var section_id = ""

    if (!src_type.equals("TOPIC_END")) {
      if (data.get("module_type") == null) {
        section_id = data.get("section_id").toString
      } else {
        section_id = data.get("module_type").toString
      }

      //section归档，联合主键: course_id topic_id section_id time
      var idsArr_section = ArrayBuffer[Any](data.get("course_id"), data.get("topic_id"), section_id, timeStr)

      //开始和结束时间在redis的缓存的时间都是时间戳
      var account_id_section_id_endtime = ""
      var endtime = ""

      var firstIntoSession = false
      //用户首次进入该阶段
      if (jedis.get(account_id+"section_id") == null) {
        firstIntoSession = true

        //缓存data中的用户阶段
        jedis.set(account_id+"section_id", section_id)

        //缓存用户_阶段_阶段开始时间
        val timeSt_str = timeSt.toString //时间戳的字符串
        jedis.set(account_id+"section_id_starttime", timeSt_str)
      }else {
        //用户进入下一个阶段
        if(!section_id.equals(jedis.get(account_id+"section_id"))){
          jedis.set(account_id+"section_id_starttime", timeSt.toString)
          jedis.set(account_id+"section_id", section_id)
        }
      }


      var flag = false //用来判断该阶段的数据在表中是否有归档
      params.clear()
      params ++= idsArr_section
      val rs = C3p0Pools.query(conn, search, params.toArray[Any])

      while (rs.next()) {
        flag = true
      }

      //判断日志类型
      //data为提交答案日志,则表示当前阶段正在进行
      if (src_type.equals("SUBMIT_QUESTION")) {

        // 根据 id 更新统计结果
        params.clear()
        params ++= idsArr_section

        val rs = C3p0Pools.query(conn, search, params.toArray[Any])

        // 当前id的归档数据已存在
        while (rs.next()) {
          if (!flag) flag = true

          var section = rs.getInt("section") //进入阶段总人次
          var sectionDone = rs.getInt("sectionDone") //完成阶段总人次
          var answer = rs.getInt("answer") //阶段下做题总数
          var answerRight = rs.getInt("answerRight") //阶段下做对题数
          var answerWrong = rs.getInt("answerWrong") //阶段下做错题数
          var incapable = rs.getInt("incapable") //阶段下不会题数
          var sawAnalysis = rs.getInt("sawAnalysis") //阶段下看过解析做题总数
          var sawRight = rs.getInt("sawRight") //阶段下看过解析做对题数
          var sawWrong = rs.getInt("sawWrong") //阶段下看过解析做错题数
          var sawUnable = rs.getInt("sawUnable") //阶段下看过解析不会题数
          var doneThemesTime = rs.getInt("doneThemesTime") //阶段下做题总用时（秒）
          var doneSectionTime = rs.getInt("doneSectionTime") //阶段完成总用时（秒）

          if (firstIntoSession) {
            /**
              * 进入阶段总人次--section
              */
            section += 1
          }


          // 用户非首次进入该阶段
          // 表示用户同一个阶段下的做题改变
          /**
            * 阶段下做题总数
            */
          answer += 1

          /**
            * 阶段下做对题数, 知识点下做错题数, 知识点下不会题数
            */
          val is_right_Data = data.get("is_right").toString
          is_right_Data match {
            case "0" => answerWrong += 1
            case "1" => answerRight += 1
            case "2" => incapable += 1
          }

          /**
            * 阶段下看过解析做题总数、做对、做错、不会
            */
          val is_view_analyze_Data = data.get("is_view_analyze").toString
          if(is_view_analyze_Data.equals("1"))sawAnalysis += 1
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
            * 阶段下做题总用时
            */
          val questionCosetime = data.get("cost_time")
          doneThemesTime += questionCosetime.toString.toInt

          //对当前id的数据进行跟新
          //"id, kp, kpdone, answer, answerRight, answerWrong, incapable, sawAnalysis, sawRight, sawWrong, sawUnable, doneThemesTime, doneKpTime, abilityValueSum, lostatus)
          params.clear()
          params ++= Array[Any](section, sectionDone, answer, answerRight, answerWrong, incapable, sawAnalysis, sawRight, sawWrong, sawUnable, doneThemesTime, doneSectionTime)
          params ++= idsArr_section
          val rs_data = C3p0Pools.execute(update, params.toArray[Any], conn)
          if (rs_data.equals(1)) println("当前id的阶段归档成功")
        }

        // rs的值为空，当前该阶段没有归档数据
        if (!flag) {
          params.clear()
          params ++= idsArr_section
          params ++= Array[Any](0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

          /**
            * 阶段,进入阶段人数+1
            */
          params.update(4, 1)

          /**
            * 阶段下做题总数+1
            */
          params.update(6, 1)

          /**
            * 阶段下做对题数, 知识点下做错题数, 知识点下不会题数
            */
          val is_right_Data = data.get("is_right").toString
          is_right_Data match {
            case "0" => params.update(8, 1)
            case "1" => params.update(7, 1)
            case "2" => params.update(9, 1)
          }

          /**
            * 阶段下看过解析做题总数、做对、做错、不会
            */
          val is_view_analyze_Data = data.get("is_view_analyze").toString
          if(is_view_analyze_Data.equals("1"))params.update(10, 1)
          is_view_analyze_Data match {
            case "0" => {
            }
            // 看过解析统计: 答对，答错，不会的人次
            case "1" => {
              is_right_Data match {
                case "0" => params.update(12, 1)
                case "1" => params.update(11, 1)
                case "2" => params.update(8, 1)
              }
            }
          }

          /**
            * 归档 阶段下做题总用时
            */
          val questionCosetime = data.get("cost_time")
          params.update(14, questionCosetime.toString.toInt)

          val rs_insert = C3p0Pools.execute(insert, params.toArray[Any], conn)
          if (rs_insert.equals(1)) println("阶段归档成功")
        }
      }

      //用户当前阶段结束
      if (src_type.equals("SECTION_END")) {

        // rs的值为空，当前该阶段没有归档数据
        if (!flag) {
          params.clear()
          params ++= idsArr_section
          params ++= ArrayBuffer[Any](0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

          val rs_insert = C3p0Pools.execute(insert, params.toArray[Any], conn)
          if (rs_insert.equals(1)) println("阶段归档成功")
        }


        /**
          * 上一个阶段已经完成，进行相应统计
          */
        params.clear()
        params ++= idsArr_section
        val rs_last = C3p0Pools.query(conn, search, params.toArray[Any])
        while (rs_last.next()) {
          var sectionDone = rs_last.getInt("sectionDone")
          var doneSectionTime_last = rs_last.getString("doneSectionTime")

          /**
            * 归档：阶段的完成人次+1
            */
          sectionDone += 1

          /**
            * 归档完成阶段的时间
            */
          val starttime = jedis.get(account_id+"section_id_starttime").toLong
          val endtime = timeSt
          doneSectionTime_last = ((endtime - starttime) / 1000).toString

          //更新表中的归档数据
          params.clear()
          params += sectionDone
          params += doneSectionTime_last
          params ++= idsArr_section

          val update_last = "update study_archive_daily_section set sectionDone = ?, doneSectionTime = ? where course_id = ? and topic_id = ? and section_id = ? and time = ?"

          val rs_update_last = C3p0Pools.execute(update_last, params.toArray[Any], conn)
          if (rs_update_last.equals(1)) println("完成上一个阶段的统计")
        }
      }
    }
    println("该条数据阶段归档结束")
  }

}

