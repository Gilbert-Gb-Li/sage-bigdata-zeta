package com.haima.sage.bigdata.etl.server

import java.net.{ServerSocket, Socket}

import  com.haima.sage.bigdata.etl.common.Constants.executor
import scala.concurrent._

/**
 * Created by zhhuiyan on 15/4/10.
 */
object SocketServer extends App{

  val server = new ServerSocket(1212)
  while(true){
    val s: Socket = server.accept()
    Future { handleClient(s) }
  }
  def handleClient(s: Socket) : Unit = {
    val in = s.getInputStream
    while(s.isConnected){
      val builder: StringBuilder = new StringBuilder
      var c= 0
     while({
       c=in.read()
       c
     } != '\n'){
       builder.append(c.toChar)
     }
      println(builder.toString())
    }
  }
}
