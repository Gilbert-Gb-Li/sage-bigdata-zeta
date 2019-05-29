package com.haima.sage.bigdata.etl.utils

import java.io.{IOException, InterruptedIOException}
import java.net._
import java.util.TimeZone

import com.haima.sage.bigdata.etl.common.Constants

/**
  * Created by liyju on 2018/1/24.
  */
object NtpUtil extends Logger{
  /**
    *
    * @return 参考时钟的秒值
    */
  def ntpServerClockMillis:Long=System.currentTimeMillis-NtpUtil.localClockOffset.toLong
   /**
    *
    * @return 本地时钟与参考时钟之间的时间差
    */
  def localClockOffset:Double={

    val ntpServer =Constants.MASTER.getString("ntp.server.address")
    val retry = Constants.MASTER.getString("ntp.server.retry").toInt.asInstanceOf[Integer]
    val port = Constants.MASTER.getString("ntp.server.port").toInt.asInstanceOf[Integer]
    val timeout = Constants.MASTER.getString("ntp.server.timeout").toInt.asInstanceOf[Integer]

    // get the address and NTP address request
    var ipv4Addr:InetAddress = null
    try {
      ipv4Addr = InetAddress.getByName(ntpServer)// 更多NTP时间服务器参考附注
    } catch  {
      case e:UnknownHostException =>
        e.printStackTrace()
    }

    var serviceStatus:Int = -1
    var socket:DatagramSocket  = null
    var localClockOffset:Double = 0
    try {
      socket = new DatagramSocket()
      socket.setSoTimeout(timeout) // will force the
      for(_ <- 0 to retry){
        if(serviceStatus != 1){
          try { // Send NTP request
            val data = new NtpMessage().toByteArray
            val outgoing = new DatagramPacket(data, data.length, ipv4Addr, port)
            socket.send(outgoing)
            // Get NTP Response
            val incoming = new DatagramPacket(data, data.length)
            socket.receive(incoming)
            TimeZone.setDefault(TimeZone.getTimeZone("GMT+8"))
            val destinationTimestamp = (System.currentTimeMillis / 1000.0) + 2208988800.0
            // 这里要加2208988800，是因为获得到的时间是格林尼治时间，所以要变成东八区的时间，否则会与与北京时间有8小时的时差
            // Validate NTP Response
            // IOException thrown if packet does not decode as expected.
            val array: Array[Byte] = incoming.getData
            val msg = new NtpMessage(((array(0) >> 6) & 0x3).toByte,((array(0) >> 3) & 0x7).toByte,(array(0) & 0x7).toByte,unsignedByteToShort(array(1)),
              array(2),array(3),(array(4) * 256.0) + unsignedByteToShort(array(5)) + (unsignedByteToShort(array(6)) / 256.0) + (unsignedByteToShort(array(7)) / 65536.0),
              (unsignedByteToShort(array(8)) * 256.0) + unsignedByteToShort(array(9)) + (unsignedByteToShort(array(10)) / 256.0) + (unsignedByteToShort(array(11)) / 65536.0),
              Array(array(12),array(13),array(14),array(15)),decodeTimestamp(array, 16),decodeTimestamp(array, 24),
              decodeTimestamp(array, 32),decodeTimestamp(array, 40))
            //数据包在客户端和服务器端之间单向传输需要1 秒。客户端和服务器端处理NTP 数据包的时间都是1 秒。
            //receiveTimestamp 此NTP 报文到达服务器端时，服务器端加上到达时间戳。
            //originateTimestamp 表示NTP 报文离开发送端时的当地时间（如T1），时间戳格式。
            //transmitTimestamp 表示远端对等体返回NTP 报文时的当地时间（如T3），时间戳格式。当对等体不可达时，该值被置为0。
            //destinationTimestamp 表示NTP 报文回到发送端时的当地时间（如T4），时间戳格式。如果本地时钟从未被同步过，值为0
            localClockOffset = ((msg.getReceiveTimestamp - msg.getOriginateTimestamp) + (msg.getTransmitTimestamp - destinationTimestamp)) / 2
            serviceStatus = 1
          } catch {
            case _: InterruptedIOException =>
            //e.printStackTrace()
          }
        }
      }
    } catch {
      case e: NoRouteToHostException =>
        logger.error(s"No route to host exception for address: $ipv4Addr" )
        e.printStackTrace()
      case e: ConnectException  =>
        // Connection refused. Continue to retry.
        logger.error(s"Connection exception for address:$ipv4Addr")
        e.printStackTrace()
      case ex: IOException =>
        logger.error(s"IOException while polling address: $ipv4Addr")
        ex.printStackTrace()
    } finally
      if (socket != null)
        socket.close()
    localClockOffset
  }
  /**
    * Converts an unsigned byte to a short. By default, Java assumes that a
    * byte is signed.
    */
  def unsignedByteToShort(b: Byte): Short = if ((b & 0x80) == 0x80) (128 + (b & 0x7f)).toShort  else b.toShort

  /**
    * Will read 8 bytes of a message beginning at <code>pointer</code> and
    * return it as a double, according to the NTP 64-bit timestamp format.
    */
  def decodeTimestamp(array: Array[Byte], pointer: Int): Double = {
    var r :Double= 0.0
    for (i  <- 0 to 7 )
      r += unsignedByteToShort(array(pointer + i)) * Math.pow(2, (3 - i) * 8)
    r
  }
}
