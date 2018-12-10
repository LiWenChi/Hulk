package up

/**
  * 将表rds_supervise.client_student_submit_answer中的数据清洗后放入rds_supervise.client_student_submit_answer_extension
  */

import utils.HdfsUtil.logger
import utils.{HdfsUtil, JsonUtil}
import com.alibaba.fastjson.{JSONArray, JSONObject}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 清洗数据存储到hdfs上
  */
object ETL_client_student_submit_answer {

  val logger = LoggerFactory.getLogger(ETL_client_student_submit_answer.getClass)

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println("Usage: HdfsTest <file>")
      System.exit(1)
    }
    val Array(inputpath, outputpath) = args
    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("HdfsTest")
      .getOrCreate()
    val file = spark.read.text(inputpath).rdd
    val outputPath = outputpath
    HdfsUtil.openHdfsFile(outputPath)
    file.foreachPartition(partition => {
      partition.foreach(Line => {
        val map: mutable.Map[String, String] = mutable.Map()
        val line = Line.mkString
        try {
          val valuesArray = ArrayBuffer[String]()
          valuesArray ++= line.split("\001").toBuffer
          if(valuesArray.size != 25){
            valuesArray ++= Array("","")
          }
          val answerJson_JsonArray = valuesArray(22).trim

          //判断answerJson_JsonArray是json串还是json数组
          var is_arrayJson = ""
          try {
            //answers字段为空
            is_arrayJson = answerJson_JsonArray.charAt(0).toString
          }catch {
            case e: Exception => {
              is_arrayJson = ""
            }
          }


          if(is_arrayJson.equals("[")){
            //answerJson_JsonArray是json串数组
            val answerJson: JSONArray = JsonUtil.getArrayFromJson(answerJson_JsonArray)
            if(answerJson.toArray().length > 1){
              answerJson.toArray().foreach(json=>{
                val valuesArray_ = ArrayBuffer[String]()
                valuesArray_ ++= valuesArray
                var line_new = ""
                val jobj = JsonUtil.getObjectFromJson(json.toString)
                val question_id = jobj.keySet().toArray()(0).toString //question_id
                val answer = jobj.get(question_id).toString //answer
                var isRightValue = ""
                try {
                  //answers字段中没有is_right
                  isRightValue = JsonUtil.getObjectFromJson(answer).get("is_right").toString
                } catch {
                  case e: Exception => {
                    isRightValue = "0"
                  }
                }
                valuesArray_ += question_id
                valuesArray_ += answer
                valuesArray_ += isRightValue

                for (item <- valuesArray_) {
                  line_new += item + "\001"
                }
                // 写入到hdfs
                HdfsUtil.writeString(line_new)
              })
            }else {
              val answerJson_ : JSONArray = JsonUtil.getArrayFromJson(answerJson_JsonArray)
              val answerJson = JsonUtil.getObjectFromJson(answerJson_.toArray()(0).toString)
              val keys = answerJson.keySet().toArray()(0).toString
              var line_new = ""
              val question_id = keys
              val answer = answerJson.get(keys).toString
              val answerValueJson = JsonUtil.getObjectFromJson(answer)
              var isRightValue = ""
              try {
                //answers字段中没有is_right
                isRightValue = answerValueJson.get("is_right").toString
              } catch {
                case e: Exception => {
                  isRightValue = "0"
                }
              }
              valuesArray += question_id
              valuesArray += answer
              valuesArray += isRightValue
              for (item <- valuesArray) {
                line_new += item + "\001"
              }
              HdfsUtil.writeString(line_new)
            }
          }else{
            //answerJson_JsonArray是json串
            val answerJson = JsonUtil.getObjectFromJson(valuesArray(22))
            var keys = answerJson.keySet().toArray()
            if (keys.size > 1) {
              for (key <- keys) {
                val valuesArray_ = ArrayBuffer[String]()
                valuesArray_ ++= valuesArray
                var line_new = ""
                val question_id = key.toString
                val answer = answerJson.get(key).toString
                val answerValueJson = JsonUtil.getObjectFromJson(answer)
                var isRightValue = ""
                try {
                  //answers字段中没有is_right
                  isRightValue = answerValueJson.get("is_right").toString
                } catch {
                  case e: Exception => {
                    isRightValue = "0"
                  }
                }
                valuesArray_ += question_id
                valuesArray_ += answer
                valuesArray_ += isRightValue

                for (item <- valuesArray_) {
                  line_new += item + "\001"
                }
                // 写入到hdfs
                HdfsUtil.writeString(line_new)
              }
            } else {
              val answerJson = JsonUtil.getObjectFromJson(valuesArray(22))
              val keys = answerJson.keySet().toArray()(0).toString
              var line_new = ""
              val question_id = keys
              val answer = answerJson.get(keys).toString
              val answerValueJson = JsonUtil.getObjectFromJson(answer)
              var isRightValue = ""
              try {
                //answers字段中没有is_right
                isRightValue = answerValueJson.get("is_right").toString
              } catch {
                case e: Exception => {
                  isRightValue = "0"
                }
              }
              valuesArray += question_id
              valuesArray += answer
              valuesArray += isRightValue
              for (item <- valuesArray) {
                line_new += item + "\001"
              }
              HdfsUtil.writeString(line_new)
            }
          }
        } catch {
          case e: Exception => {
            logger.error("[HdfsOperate]>> 该条数据为脏数据:")
            logger.error(line)
            logger.error("[HdfsOperate]>> writer a line error:", e)
          }
        }
      })
    })
    HdfsUtil.closeHdfsFile
    spark.stop()
  }
}

