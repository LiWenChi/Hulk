package up


import utils.{C3p0Pools, JsonUtil}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * sparkSQL同步hive表中的数据
  */
object StudySql {
  def main(args: Array[String]): Unit = {
    if(args.length!=2){
      println("usage:<in> <ymd>")
      System.exit(1)
    }
    val spark = SparkSession.builder().appName("studysql").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val schemaString ="env time exec_time user_id topic_id " +
      "course_id section_id course_name section_name topic_name " +
      "flow_id client_id api question_id event " +
      "onoff subject hour"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    val in = args(0)
    val ymd = args(1)
    val rdd = spark.sparkContext.textFile(in)
    val rdd1 = rdd.filter(_.split("\\s\\{").length==2).map(line=>{
      val splits = line.split("\\s\\{")
      "{" + splits(1)
    }).filter(line=>{
      var flag=true
      try{
        JsonUtil.getObjectFromJson(line)
      }catch{
        case _=>{
          flag=false
          println(line)
        }
      }
      flag
    }).map(line=>{
      val json = JsonUtil.getObjectFromJson(line)
      /* .classba.cn=off,.171xue.com=on */
      val env = json.getString("env")
      var onoff = "-1"
      if(env.endsWith(".classba.cn")){
        onoff = "off"
      }else if(env.endsWith(".171xue.com")){
        onoff = "on"
      }
      /* subject:edApp->appId */
      val edApp = json.getString("edApp")
      val json2 = JsonUtil.getObjectFromJson(edApp)
      val subject = json2.getString("appId")
      /* yyyy-MM-dd HH:mm:ss -> yyyy-MM-dd HH:mm:00 */
      val time = json.getString("time")
      val hour = time.substring(0,13)
      Row(json.getString("env"), json.getString("time"), json.getString("exec_time"), json.getString("user_id"), json.getString("topic_id"),
        json.getString("course_id"), json.getString("section_id"), json.getString("course_name"), json.getString("section_name"), json.getString("topic_name"),
        json.getString("flow_id"),  json.getString("client_id"), json.getString("api"), json.getString("question_id"), json.getString("event"),
        onoff,subject,hour)
    })
    spark.createDataFrame(rdd1,schema).createOrReplaceTempView("study")
    /* 1.	每日学习系统使用用户数:全科\分科 线上\线下 */
    var hql =
      """
        |select subject,onoff,count(distinct user_id) uv
        |from study
        |group by subject,onoff
      """.stripMargin
    spark.sql(hql).collect().foreach(row=>{
      val sb = new StringBuffer(s" INSERT INTO study_sub_onoff_num")
        .append(" (ymd,subject,onoff,user_num)")
        .append(" VALUES (?,?,?,?)")
        .append(" ON DUPLICATE KEY UPDATE update_time = NOW()")
      C3p0Pools.execute(sb.toString,Array(ymd,row.getAs[String]("subject"),row.getAs("onoff"),row.getAs("uv")))
    })

    /* 2.每日学习系统使用时长:全科\分科 线上\线下 */

    /* 3.做题时长 */
    hql =
      """
        |select user_id,question_id,abs(max(case when api='/flow/submit' then to_unix_timestamp(time,'yyyy-MM-dd HH:mm:ss') else 0 end)-min(case when api='/flow/go&&out.data.component.source.question' then to_unix_timestamp(time,'yyyy-MM-dd HH:mm:ss') else null end)) duration
        |from study
        |group by user_id,question_id
      """.stripMargin
    val df = spark.sql(hql)
    df.createOrReplaceTempView("study2")
    df.collect().foreach(row=>{
      val sb = new StringBuffer(s" INSERT INTO study_user_question_num")
        .append(" (ymd,user_id,question_id,dotime)")
        .append(" VALUES (?,?,?,?)")
        .append(" ON DUPLICATE KEY UPDATE update_time = NOW()")
      C3p0Pools.execute(sb.toString,Array(ymd,
        row.getAs[String]("user_id"),
        row.getAs("question_id"),
        row.getAs("duration").asInstanceOf[Long]/1000))
    })

    hql =
      """
        |select user_id,sum(duration) total
        |from study2
        |group by user_id
      """.stripMargin
    val df2 = spark.sql(hql).collect()
    df2.foreach(row=>{
      val sb = new StringBuffer(s" INSERT INTO study_user_num")
        .append(" (ymd,user_id,dotime)")
        .append(" VALUES (?,?,?)")
        .append(" ON DUPLICATE KEY UPDATE update_time = NOW()")
      C3p0Pools.execute(sb.toString,Array(ymd,
        row.getAs[String]("user_id"),
        row.getAs("total").asInstanceOf[Long]/1000))
    })
    val map1 = df2.map(row=>(row.getAs("user_id"),row.getAs("total").asInstanceOf[Long]/1000))
      .toMap[String,Long]
    /* 4.	平均做题时长 */
    hql =
      """
        |select user_id,count(question_id) qty
        |from study
        |group by user_id
      """.stripMargin
    val map2 = spark.sql(hql).collect().map(row=>(row.getAs("user_id"),row.getAs("qty"))).toMap[String,Long]
    map1.keySet.foreach(key=>{
      val total_time = map1.get(key).get
      val question_num = map2.get(key).get
      val avg_dotime = Math.round(total_time/question_num)
      val sb = new StringBuffer(s" INSERT INTO study_question_avg_num")
        .append(" (ymd,user_id,question_num,total_time,avg_dotime)")
        .append(" VALUES (?,?,?,?,?)")
        .append(" ON DUPLICATE KEY UPDATE update_time = NOW()")
      C3p0Pools.execute(sb.toString,Array(ymd,key,question_num,total_time,avg_dotime))
    })
    /* 5.每小时登录数 */
    hql =
      """
        |select hour,count(user_id) qty
        |from study
        |group by hour
      """.stripMargin
    spark.sql(hql).collect().foreach(row=>{
      val sb = new StringBuffer(s" INSERT INTO study_hour_login_num")
        .append(" (hour,user_num)")
        .append(" VALUES (?,?)")
        .append(" ON DUPLICATE KEY UPDATE update_time = NOW()")
      C3p0Pools.execute(sb.toString,Array(row.getAs("hour"),row.getAs("qty")))
    })

    spark.stop()
  }
}
