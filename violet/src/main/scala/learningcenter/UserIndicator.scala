package learningcenter

// $example on:spark_hive$
import java.io.File

import breeze.util.ArrayUtil
import utils.{DateUtil, JedisUtil, PropertiesUtil}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.commons.lang3.ArrayUtils

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
// $example off:spark_hive$
/**
  * 数据源表：
  * up_subject_total
  * up_total
  * up_week
  * up_rank_day
  */
object UserIndicator {

  // $example on:spark_hive$
  case class Record(key: Int, value: String)

  // $example off:spark_hive$

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("GetUserIndicator")
      .enableHiveSupport()
      .getOrCreate()

    var count = 0

    import spark.sql

    //    sql("use default")
    sql("use ads_report")

    //查询的表名
    val tableNames = PropertiesUtil.getTableNames
    //获取昨天的年月日：20181211
    val yesterday = DateUtil.getYesterday

    /**
      * 根据表名查询数据
      */
    tableNames.foreach(line => {
      val jedis = JedisUtil.getJedis(10)

      val tableName = line.toString
      //    val tableName = args(0)
      //      val tableName = "up_week"
      //            println("=================")
      //            println("=================")
      //            println("=======" + tableName + "==========")
      //            println("=======" + tableName + "==========")
      var sqlStr = ""

      if (tableName != "up_week") {
        //如果查询的表不是按周分区，则查询前一天的数据
        sqlStr = "SELECT * FROM " + tableName + " where dt = '" + yesterday + "'"
        //        sqlStr = "SELECT * FROM " + tableName + " where dt = '"+20181126+"' "
        //        sqlStr = "SELECT * FROM " + tableName + "WHERE dt =" + yesterday + "LIMIT 2"
        //        sqlStr = "SELECT * FROM default.up_subject_day WHERE user_id = \"6840100809357984\""
        //        sqlStr = "SELECT * FROM " + tableName + " LIMIT 2"
        sqlStr = sqlStr.substring(0, sqlStr.size - 2)
        sqlStr = sqlStr + "'"
      }
      if (tableName == "up_week") {
        //        //按周分区的数据，则查询所有的数据
        ////        sqlStr = "SELECT * FROM " + tableName + " LIMIT 2"
        sqlStr = "SELECT * FROM " + "up_week "
      }

      val rdd = sql(sqlStr).rdd
      val rddIte = rdd.toLocalIterator

      val rddIte_ = rdd.toLocalIterator //因为获取长度后，会清空rddIte中的数据
      val rddIte__ = rdd.toLocalIterator //因为获取长度后，会清空rddIte中的数据
      //rddIte数据的条数，在遍历数据时使用
      var step = rddIte_.size
      var step_ = rddIte__.size

      //存放同一个user_id<=>表名的所有数据
      val usersDataMap = mutable.Map[String, String]()


      /**
        * 遍历rddIte的数据
        */
      if (step_ != 0) {
        //step_ != 表示查询的结果不为空

        rddIte.foreach(line => {
          //根据数据长度，确定什么时候遍历的是最后一条数据
          step = step - 1

          /**
            * 将line转换为数组
            */
          val lineStr = line.toString()
          val lineStr_ = lineStr.substring(1, lineStr.length - 1)
          val lineArry = lineStr_.split(",")
          val array = ArrayBuffer[String]()
          array ++= lineArry
          val arr = ArrayBuffer[String]()
          var key = ""
          var value = "{"
          for (i <- array.indices) {
            arr += array(i).toString
          }


          /**
            * 将数据拼接到usersDataMap中
            * key：userid
            * value: 该userid对应的所有的数据，存储格式为json数组
            */

          /**
            * 根据数据是否为按周统计，按照user分类存储
            */
          //如果是按周分区的数据
          if (tableName == "up_week") {
            val up_subject_dayFields = PropertiesUtil.getFields("up_week")
            //arr是将表中每一天数据转换为数组
            //up_subject_dayFields是表的字段名
            for (i <- arr.indices) {
              if (i == 0) {
                key = arr(i) + "<=>" + arr(arr.length - 1) //userId\001dt userId和周数
                value = value + "\"" + up_subject_dayFields(i) + "\"" + ":" + "\"" + arr(i) + "\"" + ","
              } else if (i < arr.size - 1) {
                value = value + "\"" + up_subject_dayFields(i) + "\"" + ":" + "\"" + arr(i) + "\"" + ","
              }
              if (i == arr.size - 1) {
                value = value + "\"" + up_subject_dayFields(i) + "\"" + ":" + "\"" + arr(i) + "\"" + "}"
              }
            }

            if (step != 0) {
              //step != 0 该条数据不是最后一条数据
              if (!usersDataMap.contains(key)) {
                val tmp = "[" + value + ","
                usersDataMap += (key -> tmp)
              } else {
                var userData = usersDataMap.get(key).toString
                val tmp = value + ","
                userData = userData + tmp
                usersDataMap += (key -> userData)
              }
            } else {
              //否则该条记录是最后一条记录
              if (!usersDataMap.contains(key)) {
                val tmp = "[" + value + "]"
                usersDataMap += (key -> tmp)
              } else {
                var userData = usersDataMap.get(key).toString
                val tmp = value + "]"
                userData = userData + tmp
                usersDataMap += (key -> userData)
              }

              /**
                * 对map中的数据进行清洗，去除多余的字符,转换为json数组
                */
              var keysArr = usersDataMap.keySet.toArray

              //如果数据中只有一个key，则不删除key
              if (keysArr.size > 1) {
                val thisKeyIndex = keysArr.indexOf(key)
                keysArr = ArrayUtils.remove(keysArr, thisKeyIndex)
              }
              val removeStr = "Some"
              val removeStr1 = ")"
              val removeStr2 = "("
              for (i <- keysArr.indices) {
                var data = usersDataMap.get(keysArr(i)).toString
                data = data.replace(removeStr, "").replace(removeStr1, "").replace(removeStr2, "").replace("},", "}]").replace("}]{", "},{")
                usersDataMap += (keysArr(i) -> data)
                //                println("============输出每个key:" + keysArr(i) + "清洗后的value:=====================")
                //                println(data)
              }
            }


          }


          //如果不是按周分区的数据
          if (tableName != "up_week") {
            val up_subject_dayFields = PropertiesUtil.getFields(tableName)
            for (i <- arr.indices) {
              if (i == 0) {
                key = arr(i)
                value = value + "\"" + up_subject_dayFields(i) + "\"" + ":" + "\"" + arr(i) + "\"" + ","
              } else if (i < arr.size - 1) {
                value = value + "\"" + up_subject_dayFields(i) + "\"" + ":" + "\"" + arr(i) + "\"" + ","
              }
              if (i == arr.size - 1) {
                value = value + "\"" + up_subject_dayFields(i) + "\"" + ":" + "\"" + arr(i) + "\"" + "}"
              }
            }
            if (step != 0) {
              //step != 0 该条数据不是最后一条数据
              if (!usersDataMap.contains(key)) {
                val tmp = "[" + value + ","
                usersDataMap += (key -> tmp)
              } else {
                var userData = usersDataMap.get(key).toString
                val tmp = value + ","
                userData = userData + tmp
                usersDataMap += (key -> userData)
              }
            } else {
              //否则该条记录是最后一条记录
              if (!usersDataMap.contains(key)) {
                val tmp = "[" + value + "]"
                usersDataMap += (key -> tmp)
              } else {
                var userData = usersDataMap.get(key).toString
                val tmp = value + "]"
                userData = userData + tmp
                usersDataMap += (key -> userData)
              }

              /**
                * 对map中的数据进行清洗，去除多余的字符,转换为json数组
                */
              var keysArr = usersDataMap.keySet.toArray

              //如果数据中只有一个key，则不删除key
              if (keysArr.size > 1) {
                val thisKeyIndex = keysArr.indexOf(key)
                keysArr = ArrayUtils.remove(keysArr, thisKeyIndex)
              }
              val removeStr = "Some"
              val removeStr1 = ")"
              val removeStr2 = "("
              for (i <- keysArr.indices) {
                var data = usersDataMap.get(keysArr(i)).toString
                data = data.replace(removeStr, "").replace(removeStr1, "").replace(removeStr2, "").replace("},", "}]").replace("}]{", "},{")
                usersDataMap += (keysArr(i) -> data)
                //                println("============输出每个key:" + keysArr(i) + "清洗后的value:=====================")
                //                println(data)
              }
            }
          }



          //          println("key:" + key)
          //          println("value:" + value)
          //          println("=================")
          //          println("=================")
          /** rdd中的数据遍历存储到usersDataMap中结束 **/
        })


        /** 将数据存储redis中 **/
        /**
          * 区分数据源是否是按周分区数据
          *
          * 将usersDataMap中的数据存储到redis中
          * key：userid_tableName
          * value：map中的value
          */
        //        println("=======输出存储到redis中map的所有数据==========")
        if (tableName != "up_week") {
          usersDataMap.foreach(line => {
            var lineStr = line.toString()
            lineStr = lineStr.replace("(", "").replace(")", "")
            val index = lineStr.indexOf(',')
            val key = lineStr.substring(0, index) + "<=>" + tableName
            val value = lineStr.substring(index + 1)
            jedis.set(key, value)
            //                        println("key:" + key)
            //                        println("value:" + jedis.get(key))
          })
        }

        if (tableName == "up_week") {
          usersDataMap.foreach(line => {
            //            println("==========遍历usersDataMap中的数据==========")
            var lineStr = line.toString()
            lineStr = lineStr.replace("(", "").replace(")", "")
            val index = lineStr.indexOf(',')
            val k = lineStr.substring(0, index) //k: userid_周数
            val userId_dt = k.split("<=>")
            val userId = userId_dt(0)
            val dt = userId_dt(1)
            //            println("k:" + k)
            //            println("userId_dt:" + userId_dt.foreach(println))
            //            println("userId:" + userId)
            //            println("dt:" + dt)

            //            val key = lineStr.substring(0, index) + "<=>" + tableName
            val key = userId + "<=>" + tableName
            val value = lineStr.substring(index + 1)
            val score = dt.toDouble
            //            jedis.set(key, value)
            jedis.zadd(key, score, value)
          })
        }

        //        println("=================")
        //        println("=================")
        //        println("=================")
        //        println("=================")


      }
    })

    spark.stop()
  }
}

