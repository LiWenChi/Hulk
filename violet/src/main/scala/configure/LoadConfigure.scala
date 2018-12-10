package configure

import java.net.InetAddress

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/**
  * load configure file
  */
object LoadConfigure {

  def getBrokers(): String = {
    val map = getConfigs()
    map.get("brokers").get
  }

  def getHbaseConfiguration(): Configuration = {
    val hbaseConf = HBaseConfiguration.create()
    val map = getConfigs()
    val quorum = map.get("hbase.zookeeper.quorum").get
    val port = map.get("hbase.zookeeper.property.clientPort").get
    hbaseConf.set("hbase.zookeeper.quorum", quorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", port)
    hbaseConf.set("hbase.client.keyvalue.maxsize","524288000");//最大500m
    return hbaseConf
  }

  def getConfigs(): Map[String, String] = {
    val ip = getIP()
    val configFile = ip match {
      case _ if ip.startsWith(IpEnum.DevelopEnv.toString) => "common_test.properties"
      case _ if ip.startsWith(IpEnum.TestEnv.toString) => "common_test.properties"
      case _ if ip.startsWith(IpEnum.FormalEnv.toString) => "common_formal.properties"
    }

    return Source.fromInputStream(
      this.getClass.getClassLoader.getResource(configFile).openStream()
    ).getLines().filterNot(_.startsWith("#")).map(_.split("=")).map(x => (x(0), x(1))).toMap
  }

  def getIP(): String = {
    return InetAddress.getLocalHost().getHostAddress()
  }

  def getC3po(): String ={
    val map = getConfigs()
    map.get("c3p0.file").get
  }
  def getRedisArr():ArrayBuffer[String]={
    val map = getConfigs()
    val arr = new ArrayBuffer[String]()
    arr.append(map.get("redis.host").get)
    arr.append(map.get("redis.passwd").get)
    arr
  }
}

