package com.haima.sage.bigdata.etl.utils

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date
/**
  * Created by liyju on 2018/1/24.
  * @param leapIndicator 值为“11”时表示告警状态，时钟不能被同步。为其他值时NTP 本身不做处理
  * @param version NTP 的版本号，目前值为3
  * @param mode NTP 的工作模式。不同值表示的含义如下：0：reserved，保留。1：symmetric active，主动对等体模式。2：symmetric passive，被动对等体模式。
  *             3：client，客户模式。4：server，服务器模式。5：broadcast，广播模式。6：reserved for NTP control messages，NTP 控制报文。7：reserved for private use，内部使用预留。
  * @param stratum 时钟的层数，定义了时钟的准确度。层数为1 的时钟准确度最高，从1 到15 依次递减。
  * @param pollInterval 轮询时间，即发送报文的最小间隔时间。
  * @param precision 时钟的精度
  * @param rootDelay 到主参考时钟的总往返延迟时间。
  * @param rootDispersion 本地时钟相对于主参考时钟的最大误差。
  * @param referenceIdentifier  标识特定参考时钟
  * @param referenceTimestamp  本地时钟最后一次被设定或更新的时间。如果值为0 表示本地时钟从未被同步过。
  * @param originateTimestamp NTP 报文离开源端时的本地时间
  * @param receiveTimestamp NTP 报文到达目的端的本地时间
  * @param transmitTimestamp 目的端应答报文离开服务器端的本地时间
  */
class NtpMessage(leapIndicator:Byte = 0, version:Byte = 3, mode:Byte = 0, stratum:Short = 0, pollInterval:Byte=0,
                  precision:Byte = 0 , rootDelay:Double=0, rootDispersion:Double=0, referenceIdentifier:Array[Byte]=Array[Byte](0,0,0,0),
                  referenceTimestamp:Double=0, originateTimestamp:Double=0, receiveTimestamp:Double=0, transmitTimestamp:Double=0) {

 /**
    * Constructs a new NtpMessage in client -> server mode, and sets the
    * transmit timestamp to the current time.
    */
  def this() {
    // Note that all the other member variables are already set with
    // appropriate default values.
    this(mode=3,transmitTimestamp=(System.currentTimeMillis() / 1000.0) + 2208988800.0)
  }

  /**
    * This method constructs the data bytes of a raw NTP packet.
    */
  def toByteArray: Array[Byte] = { // All bytes are automatically set to 0
    val p = new Array[Byte](48)
    p(0) = (leapIndicator << 6 | version << 3 | mode).asInstanceOf[Byte]
    p(1) = stratum.asInstanceOf[Byte]
    p(2) = pollInterval.asInstanceOf[Byte]
    p(3) = precision.asInstanceOf[Byte]
    // root delay is a signed 16.16-bit FP, in Java an int is 32-bits
    val l = (rootDelay * 65536.0).asInstanceOf[Int]
    p(4) = ((l >> 24) & 0xFF).toByte
    p(5) = ((l >> 16) & 0xFF).toByte
    p(6) = ((l >> 8) & 0xFF).toByte
    p(7) = (l & 0xFF).toByte
    // root dispersion is an unsigned 16.16-bit FP, in Java there are no
    // unsigned primitive types, so we use a long which is 64-bits
    val ul = (rootDispersion * 65536.0).asInstanceOf[Long]
    p(8) = ((ul >> 24) & 0xFF).toByte
    p(9) = ((ul >> 16) & 0xFF).toByte
    p(10) = ((ul >> 8) & 0xFF).toByte
    p(11) = (ul & 0xFF).toByte
    p(12) = referenceIdentifier(0)
    p(13) = referenceIdentifier(1)
    p(14) = referenceIdentifier(2)
    p(15) = referenceIdentifier(3)
    encodeTimestamp(p, 16, referenceTimestamp)
    encodeTimestamp(p, 24, originateTimestamp)
    encodeTimestamp(p, 32, receiveTimestamp)
    encodeTimestamp(p, 40, transmitTimestamp)
    p
  }


  /**
    * Returns a string representation of a NtpMessage
    */
  override def toString: String = {
    val precisionStr = new DecimalFormat("0.#E0").format(Math.pow(2,precision))
     val str = s"Leap indicator: $leapIndicator Version: $version Mode: $mode Stratum: $stratum Poll: $pollInterval Precision: $precision " +
       s" ($precisionStr seconds) Root delay: ${new DecimalFormat("0.00").format(rootDelay * 1000)} ms Reference identifier: " +
       s"${referenceIdentifierToString(referenceIdentifier, stratum,version)} Reference timestamp: ${timestampToString(referenceTimestamp)} " +
       s"Originate timestamp: ${timestampToString(originateTimestamp)} Receive timestamp: ${timestampToString(receiveTimestamp)} " +
       s"Transmit timestamp: ${timestampToString(transmitTimestamp)}"
    str
   }

  /**
    * Encodes a timestamp in the specified position in the message
    */
  def encodeTimestamp(array: Array[Byte], pointer: Int, timestamp: Double): Unit = { // Converts a double into a 64-bit fixed point

    var timestampTemp: Double = timestamp
    for ( i <- 0 to 7) {
      // 2^24, 2^16, 2^8, .. 2^-32
      val base:Double = Math.pow(2, (3 - i) * 8)
      // Capture byte value
      array(pointer + i) =(timestampTemp / base).toByte
      // Subtract captured value from remaining total
        timestampTemp = timestampTemp- (unsignedByteToShort(array(pointer + i)) * base)
    }
    // From RFC 2030: It is advisable to fill the non-significant low order bits of the timestamp with a random, unbiased
    // bitstring, both to avoid systematic roundoff errors and as a means of loop detection and replay detection.
    array(7) = (Math.random * 255.0).toByte
  }

  /**
    * Returns a timestamp (number of seconds since 00:00 1-Jan-1900) as a formatted date/time string.
    */
  def timestampToString(timestamp: Double): String = {
    if (timestamp == 0) return "0"
    // timestamp is relative to 1900, utc is used by Java and is relative to 1970
    val utc = timestamp - 2208988800.0
    // milliseconds
    val ms = (utc * 1000.0).toLong
    // date/time
    val date = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss").format(new Date(ms))
    // fraction
    val fraction = timestamp - timestamp.toLong
    val fractionSting = new DecimalFormat(".000000").format(fraction)
    date + fractionSting
  }

  /** */
  /**
    * Returns a string representation of a reference identifier according to
    * the rules set out in RFC 2030.
    */
  def referenceIdentifierToString(ref: Array[Byte], stratum: Short, version: Byte): String = { // From the RFC 2030:
    // In the case of NTP Version 3 or Version 4 stratum-0 (unspecified)
    // or stratum-1 (primary) servers, this is a four-character ASCII
    // string, left justified and zero padded to 32 bits.
    if (stratum == 0 || stratum == 1)
      new String(ref)
    // In NTP Version 3 secondary servers, this is the 32-bit IPv4
    // address of the reference source.
    else  if (version == 3)
        unsignedByteToShort(ref(0)) + "." + unsignedByteToShort(ref(1)) + "." + unsignedByteToShort(ref(2)) + "." + unsignedByteToShort(ref(3))
    // In NTP Version 4 secondary servers, this is the low order 32 bits
    // of the latest transmit timestamp of the reference source.
    else  if (version == 4)
          "" + ((unsignedByteToShort(ref(0)) / 256.0) + (unsignedByteToShort(ref(1)) / 65536.0) + (unsignedByteToShort(ref(2)) / 16777216.0) + (unsignedByteToShort(ref(3)) / 4294967296.0))
    else
          ""
  }
  /**
    * Converts an unsigned byte to a short. By default, Java assumes that a
    * byte is signed.
    */
  def unsignedByteToShort(b: Byte): Short = if ((b & 0x80) == 0x80) (128 + (b & 0x7f)).toShort  else b.toShort

  def getReceiveTimestamp: Double = receiveTimestamp
  def getOriginateTimestamp: Double =originateTimestamp
  def getTransmitTimestamp: Double =transmitTimestamp

}
