package utils


import java.util._

object PropertiesUtil {

  val  tablename_fieldProp = new Properties()

  val tablename_field = classOf[PropertiesUtil].getClassLoader.getResourceAsStream("tablename_field.properties")
  try {
    tablename_fieldProp.load(tablename_field)
  } catch {
    case e : Exception =>{
      println(e)
    }
  }

  /**
    * 根据表名获取字段
    * @return
    */
  def getFields (key: String): Array[String] = {
    val fields = tablename_fieldProp.getProperty(key)
    val fieldsArr = fields.split(",")
    fieldsArr
  }

  /**
    * 获取所有的表名
    * @return
    */
  def getTableNames : Array[AnyRef] = {
    tablename_fieldProp.keySet().toArray
  }
}

class PropertiesUtil

