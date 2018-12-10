package utils


import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JsonUtil {
  val mapper: ObjectMapper = new ObjectMapper()

  def toJsonString(T: Object): String = {
    // 注册module
    mapper.registerModule(DefaultScalaModule)
    mapper.writeValueAsString(T)
  }

  def getArrayFromJson(jsonStr: String) = {
    JSON.parseArray(jsonStr)
  }

  def getObjectFromJson(jsonStr: String) = {
    JSON.parseObject(jsonStr)
  }

  def main(args: Array[String]) {
    val json = "[{\"batchid\":305322456,\"amount\":20.0,\"count\":20},{\"batchid\":305322488,\"amount\":\"10.0\",\"count\":\"10\"}]"
    val array: JSONArray = JsonUtil.getArrayFromJson(json)
    println(array)
    array.toArray().foreach(json=>{
      println(json)
      val jobj = json.asInstanceOf[JSONObject]
      println(jobj.get("batchid"))
    })

    val jsonStr = "{\"batchid\":119,\"amount\":200.0,\"count\":200}"
    val jsonObj: JSONObject = JsonUtil.getObjectFromJson(jsonStr)
    println(jsonObj)

  }
}
