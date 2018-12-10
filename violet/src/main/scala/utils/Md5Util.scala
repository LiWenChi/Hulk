package utils

import java.security.MessageDigest

/**
  * description: 拼接的key过长，转md5
  * author: wang cun xin
  * date: 2018/10/16 14:12
  */
object Md5Util {
  val digest = MessageDigest.getInstance("MD5")

  def md5(text : String): String={
    val result = this.digest.digest(text.getBytes).map("%02x".format(_)).mkString
    result
  }

}
