package utils


import configure.LoadConfigure
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
/**
  * description: redis pool util
  */
object JedisUtil {
  val arr = LoadConfigure.getRedisArr()
  val host = arr(0)
  val passwd = arr(1)
  val jedisPoolConfig = new JedisPoolConfig
  jedisPoolConfig.setMaxIdle(10)
  jedisPoolConfig.setMaxWaitMillis(2000)
  jedisPoolConfig.setMaxTotal(200)
  var pool:JedisPool= null
  registerShutdownHook

  /**
    * 从jedis pool获取jedis对象
    * @return
    */
  def getJedis(db :Int): Jedis={
    if(pool ==null){
      pool = new JedisPool(jedisPoolConfig,host,6379,2000,passwd,db)
    }
    val jedis = pool.getResource()
    jedis
  }

  /**
    * 放回jedis到连接池
    * @param jedis
    */
  def retJedis(jedis:Jedis): Unit ={
    try {
      if (jedis != null) {
        jedis.close()
      }
    } catch {
      case e:Exception=>println(e)
    }
  }

  private[this] def registerShutdownHook(): Unit = {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        try{
          if(pool!=null){
            pool.destroy()
            println("release jedis pool")
          }
        }catch {
          case e:Exception=>println("failed to release pool",e)
        }
      }
    })
  }

  def main(args: Array[String]): Unit = {
    val jedis = JedisUtil.getJedis(0)
    jedis.set("k2","v2")
    println(jedis.get("k2"))
    JedisUtil.retJedis(jedis)
  }
}
