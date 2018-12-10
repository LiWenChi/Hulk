package utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.commons.lang.time.DateUtils


import scala.collection.mutable.ArrayBuffer

/**
  * description: 日期工具类
  * author: wang cun xin
  * date: 2018/10/11 18:56
  */
object DateUtil {
  /**
    * 时间戳转日期字符串
    * @param ts
    * @return yyyy-MM-dd HH:mm:ss
    */
  def str2Date(ts: Long): String = {
    val date = new Date(ts)
    val simpleDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    simpleDate.format(date)
  }

  /**
    * 日期类型转字符串
    * @param date
    * @return yyyy-MM-dd HH:mm:ss
    */
  def getStrDateTime(date: Date): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //df.setTimeZone(TimeZone.getTimeZone("GMT+8"));
    df.format(date)
  }
  /**
    * 比较日期
    * @param array 日期数组
    * @return 最小的日期
    */
  def compareDate(array: ArrayBuffer[String]): String = {
    array.filter(a => !a.equals("") && !a.isEmpty ).reduce((a1, a2) => if(a1>a2) a2 else a1 )
  }

  /**
    * 字符串日期格式转时间戳
    * @param str:yyyy-MM-dd HH:mm:ss
    * @return long
    */
  def str2ts(str:String)={
    val calendar = Calendar.getInstance
    calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(str))
    calendar.getTimeInMillis
  }

  /**
    * 获取昨天的时间
    * @return
    */
  def getYesterday : String = {
    import java.text.SimpleDateFormat
    import java.util.Calendar
    val cal = Calendar.getInstance
    cal.add(Calendar.DATE, -1)
    val yesterday = new SimpleDateFormat("yyyyMMdd ").format(cal.getTime)
    yesterday
  }

  /**
    * 获取当前时间所在的周数
    */
  def getWeek = {
    import java.util.Calendar
    val cal = Calendar.getInstance
    val week = cal.getWeeksInWeekYear
    System.out.println("今天是今年的第" + week + "周;")
  }
}
