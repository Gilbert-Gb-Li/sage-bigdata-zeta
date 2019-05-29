package com.haima.sage.bigdata.etl.utils

import java.security.MessageDigest

object UUIDUtils {
  private val hexDigits = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F')
  private val mdInst = new ThreadLocal[MessageDigest]() {
    override def initialValue(): MessageDigest = {
      MessageDigest.getInstance("MD5")
    }
  }
  //private val mdInst = MessageDigest.getInstance("MD5")

  def id(id: String): String = {
    new String(bytesId(id.getBytes))
  }

  def bytesId(id: Array[Byte]): Array[Char] = {

    val md = mdInst.get().digest(id)
   // val md = mdInst.digest(id)
    // 把密文转换成十六进制的字符串形式
    val j = md.length
    (0 until j).map(i =>
      List(hexDigits(md(i) >>> 4 & 0xf), hexDigits(md(i) & 0xf))).reduce[List[Char]] {
      case (a, b) =>
        b ::: a
    }.toArray


  }


}
