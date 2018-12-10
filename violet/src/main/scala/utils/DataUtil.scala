package utils

import com.alibaba.fastjson.JSONObject
import redis.clients.jedis.Jedis

/**
  * Created by LiWenChi on 2018/10/26.
  */
object DataUtil {

  /**
    * 获取日志中的年月日字符串
    *
    * @return 年月日字符串
    */
  def getTimeSre(line: String): String = {
    val index = line.indexOf('{')
    val jsonStr = line.substring(index)
    val jsonObj: JSONObject = JsonUtil.getObjectFromJson(jsonStr)

    // 时间
    val time = jsonObj.get("time").toString //2018-10-12 14:36:48
    val timeStr = time.substring(0, 10).replace("-", "") //20181012
    timeStr
  }

  /**
    * 清楚缓存中昨天的数据
    */
  def cleanCache(jedis: Jedis, line: String): Unit = {
    //缓存清理
    val timeDate = getTimeSre(line).toString //20181012
    if(jedis.get("date") == null){
      jedis.set("date", timeDate)
    }
    val timeCache = jedis.get("date")
    if(!timeCache.equals(timeDate)){
      val res = jedis.flushDB()
      println(timeCache+"的缓存清理成功"+res)
      jedis.set("date",timeDate)
    }
  }
  /**
    * 过滤脏数据
    * @return
    */
  def isNormative(line: String): Boolean = {
    try {
      val index = line.indexOf('{')
      val jsonStr = line.substring(index)
      val jsonObj: JSONObject = JsonUtil.getObjectFromJson(jsonStr)
    } catch {
      case e:Exception =>
        e.printStackTrace()
        return false
    }
    true
  }
}

