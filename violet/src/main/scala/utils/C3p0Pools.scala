package utils


import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties

import configure.LoadConfigure
import com.mchange.v2.c3p0.ComboPooledDataSource

import scala.collection.mutable.ArrayBuffer

object C3p0Pools extends Serializable{

  private var dataSource:ComboPooledDataSource = _
  init()
  registerShutdownHook

  private def init() {
    try {
      //自动获取c3p0.properties配置项
      dataSource = new ComboPooledDataSource();
      val prop = new Properties();
      val in = this.getClass.getClassLoader().getResourceAsStream(LoadConfigure.getC3po());
      prop.load(in);
      dataSource.setDriverClass(prop.getProperty("c3p0.driverClass"));
      dataSource.setJdbcUrl(prop.getProperty("c3p0.jdbcUrl"));
      dataSource.setUser(prop.getProperty("c3p0.user"));
      dataSource.setPassword(prop.getProperty("c3p0.password"))
      // dataSource.setMaxPoolSize(prop.getProperty("c3p0.maxPoolSize").toInt)
    } catch {
      case e:Exception =>
        e.printStackTrace();
    }
  }


  def getConnection(isAuto:Boolean=true): Connection = {
    try {
      val conn = dataSource.getConnection
      conn.setAutoCommit(isAuto)
      return conn
    } catch {
      case e:Exception =>
        e.printStackTrace()
        return null
    }
  }

  /**
    * 执行查询 （ 查询链接需要手动释放 ）
    *
    * @param sql
    * @param params
    * @return
    */
  def query(conn:Connection, sql:String , params: Array[Any] = null): ResultSet= {
    var pstmt:PreparedStatement = null
    var rs:ResultSet = null
    try{
      pstmt = conn.prepareStatement( sql )

      if (params != null) {
        ( 0 until params.length ).foreach(
          i =>{
            pstmt.setObject( i+1 , params(i) )
          }
        )
      }
      rs = pstmt.executeQuery
      // pstmt.close()
      rs
    }catch{
      case e:Exception => {
        e.printStackTrace
        throw new Exception
      }
    }
  }

  /**
    * insert update
    *
    * @param sql
    * @param params
    * @param outConn  缺省Connection 如果为 AutoCommit则无需传入
    * @return
    */
  def execute(sql:String, params:Array[Any], outConn:Connection=null): Int = {
    // 若未传入connection 表示AutoCommit = true
    var conn = outConn
    if (conn == null) {
      conn = getConnection()
    }
    var count = 0
    try {
      var psm: PreparedStatement = conn.prepareStatement(sql)
      if (params != null) {
        for ( i <- params.indices) psm.setObject(i + 1, params(i))
      }
      count = psm.executeUpdate()
      //      psm.close()
    } catch {
      case e:Exception => {
        e.printStackTrace
        throw new Exception
      }
    }
    count
  }

  def closeConnecton(conn:Connection): Unit = {
    if (conn != null && conn.getAutoCommit) {
      conn.close()
    }
  }

  private[this] def registerShutdownHook(): Unit ={
    Runtime.getRuntime.addShutdownHook( new Thread(){
      override def run(): Unit = {
        try{
          println("C3p0Pools release .... ")
          dataSource.close()
        } catch {
          case e:Exception =>
            e.printStackTrace()
        }

      }
    } )
  }

  def main(args: Array[String]): Unit = {
    for (i <- 0 to 10){
      val conn = getConnection()
      println(conn)
      conn.close()
    }
    Thread.sleep(10000)
  }

}

