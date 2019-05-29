package com.haima.sage.bigdata.etl.utils

import java.net.{InetSocketAddress, _}
import java.nio.channels.SocketChannel

import com.haima.sage.bigdata.etl.common.model.{Protocol, TCP, WithProtocol}

case class NetUtils(host: String, port: Int, implicit val protocol: Protocol = TCP()) {
  private val timeout = 50

  import scala.collection.JavaConversions._


  def availableNetwork: (Boolean, Option[NetworkInterface], Option[String]) = {
    try {

      val address = InetAddress.getByName(host); //ping this IP
      address match {
        case _: java.net.Inet4Address | _: java.net.Inet6Address =>
          NetworkInterface.getNetworkInterfaces.find(addr => {
            address.isReachable(addr, 0, 5000)
          }) match {
            case Some(addr) =>
              (true, Some(addr), None)
            case _ =>
              (false, None, Some(s" all local Network cat`t connect to $host"))
          }
        case _ =>
          (false, None, Some(host + " is unrecongnized"))
      }


    } catch {
      case e: Exception =>
        System.out.println("error occurs.")
        e.printStackTrace()
        (false, None, Some(e.getMessage))
    }
  }

  def isTcp(implicit protocol: Protocol): Boolean = {
    protocol match {
      case _: TCP =>
        true
      case d: WithProtocol =>
        isTcp(d.protocol)
      case _ =>
        false
    }
  }

  /*
  *
  *
  * */
  def connectAble: (Boolean, String) = {


    availableNetwork match {
      case (true, _, _) =>
        try {
          if (isTcp) {
            val channel = SocketChannel.open
            val flag = channel.connect(new InetSocketAddress(host, port))

            channel.close()
            (flag, "")
          } else {
            /* FIXME udp is not check able
              val channel = DatagramChannel.open
              val remoteAddress = new InetSocketAddress(host, port)
              val connect = channel.connect(remoteAddress)
              val flag = connect.isConnected
              connect.disconnect()
              channel.close()*/
            (true, "")
          }
        } catch {
          case e: Exception =>
            (false, e.getMessage)
        }


      case (false, _, msg) =>
        (false, msg.get)
    }
  }


  def bindAble: (Boolean, String) = {
    if (isTcp) {
      try {
        val address = new InetSocketAddress(host, port)
        val socket = new ServerSocket
        socket.setSoTimeout(timeout)
        socket.bind(address)
        socket.close()
        (true, "")
      } catch {
        case e: java.net.BindException =>
          (false, e.getMessage)
        case e: Exception =>
          (false, e.getMessage)
      }
    } else {
      try {
        val address = new InetSocketAddress(host, port)
        val socket = new DatagramSocket(null)
        socket.setSoTimeout(timeout)
        socket.bind(address)
        socket.close()
        (true, "")
      } catch {
        //只有出现绑定异常的时候，认定用户输入的ip错误或端口已占用
        case e: java.net.BindException =>
          (false, e.getMessage)
        case e: Exception =>
          (false, e.getMessage)
      }
    }


  }


}
