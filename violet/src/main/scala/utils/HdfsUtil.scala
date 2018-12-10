package utils

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.web.resources.ExceptionHandler
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.io._
import java.net.URI

object HdfsUtil {

  var conf = new Configuration()
  var writer : BufferedWriter = null
  val logger  = LoggerFactory.getLogger(HdfsUtil.getClass)
  /**
    * 在hdfs上创建一个输出流
    * @param path 文件的路径
    */
  def openHdfsFile(path:String): Unit= {

    val fs = FileSystem.get(URI.create(path),conf, "hdfs")
    writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(path))))
    if(null!=writer){
      logger.info("==========================================")
      logger.info("[HdfsOperate]>> initialize writer succeed!")
      logger.info("==========================================")
    }
  }

  /**
    * 往hdfs文件中写入数据
    * @param line 输入的数据
    */
  def writeString(line: String):Unit = {
    try {
      writer.write(line + "\n")
    } catch {
      case e : Exception => {
        logger.info("==========================================")
        logger.error("[HdfsOperate]>> writer a line error:", e)
        logger.info("==========================================")
      }
    }
  }

  /**
    * 关闭hdfs输出流
    */
  def closeHdfsFile:Unit = {
    try {
      if (null != writer) {
        writer.close()
        logger.info("==========================================")
        logger.info("[HdfsOperate]>> closeHdfsFile close writer succeed!")
        logger.info("==========================================")
      }
      else {
        logger.info("==========================================")
        logger.error("[HdfsOperate]>> closeHdfsFile writer is null")
        logger.info("==========================================")
      }
    } catch {
      case e : Exception => {
        logger.info("==========================================")
        logger.error("[HdfsOperate]>> closeHdfsFile close hdfs error:", e)
        logger.info("==========================================")
      }
    }
  }
}

