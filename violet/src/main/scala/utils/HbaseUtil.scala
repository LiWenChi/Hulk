package utils


import configure.LoadConfigure
import org.apache.hadoop.hbase.{Cell, CellUtil, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer

/**
  * hbase client 单实例实现读写
  */
object HbaseUtil {
  private val conf = LoadConfigure.getHbaseConfiguration()
  private val conn = ConnectionFactory.createConnection(conf)
  private var hTable: Table = null
  println("HbaseClient init")

  def init(tableName: String) {
    if(hTable==null){
      hTable = conn.getTable(TableName.valueOf(tableName))
      sys.addShutdownHook {
        try {
          println("hbase release ...")
          if (hTable != null) {
            hTable.close()
          }
          if (conn != null) {
            conn.close()
          }
        } catch {
          case e: Exception => println(s"fail to close:${e}")
        }
      }
    }else{
      println("htable has already initialized")
    }
  }

  def put(rowkey: String, cf: String, array:ArrayBuffer[(String,AnyRef)]): Unit = {
    val p = new Put(rowkey.getBytes())
    array.foreach( x => {
      p.addColumn(cf.getBytes, x._1.toString.getBytes, x._2.toString.getBytes)
    })
    hTable.put(p)
  }

  def getDataBuffer(rowKey: String, cf: String): ArrayBuffer[(String, String)] = {
    val get = new Get(Bytes.toBytes(rowKey))
    get.addFamily(Bytes.toBytes(cf))
    val rs = hTable.get(get)
    var result = ArrayBuffer[(String, String)]()
    for (cell <- rs.rawCells()) {
      val value = Bytes.toString(CellUtil.cloneValue(cell))
      val key = Bytes.toString(CellUtil.cloneQualifier(cell))
      result.+=((key, value))
    }
    result
  }

  def getDataMap(rowKey: String, cf: String): Map[String, String] = {
    val get = new Get(Bytes.toBytes(rowKey))
    get.addFamily(Bytes.toBytes(cf))
    val rs = hTable.get(get)
    var resMap: Map[String, String] = Map()
    for (cell <- rs.rawCells()) {
      val c: Cell = cell
      val value = Bytes.toString(CellUtil.cloneValue(cell))
      val key = Bytes.toString(CellUtil.cloneQualifier(cell))
      resMap += (key -> value)
    }
    resMap
  }

  def main(args: Array[String]): Unit = {
    HbaseUtil.init("tb_test")
  }
}

