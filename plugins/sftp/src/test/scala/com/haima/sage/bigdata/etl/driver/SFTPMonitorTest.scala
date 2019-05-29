package com.haima.sage.bigdata.etl.driver

import java.util.Properties

import akka.actor.{Actor, ActorSystem, Props}
import com.haima.sage.bigdata.etl.codec.DelimitCodec
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.monitor.file.SFTPMonitor

/**
  * Created by bbtru on 2017/9/29.
  */
object SFTPMonitorTest extends App {

  class SftpMonitorActor extends Actor {

    //如果assert断言失败会抛 java.lang.AssertionError: assertion failed
    val host = "10.10.100.58"
    val port = Some(22)
    val path = "/home/bigdata/sftp/backup"
    val skip = 0
    val user = "bigdata"
    val password = "bigdata"
    var numLine = 0

    def receive = {
      case Opt.START =>
        //FileSource
        val fileSource = new FileSource(path, Some("other"),
          Some(new TxtFileType()), None, Some(new DelimitCodec("delimit")), Some(ProcessFrom.END), skip)
        //Properties
        val properties = new Properties()
        properties.setProperty("user", user)
        properties.setProperty("password", password)
        //SFTPSource
        val sftpSource =  SFTPSource(host, port, fileSource, Some(properties))
        val paser = new NothingParser(null, Some(List()))
        val listener = context.actorOf(Props.create(classOf[SFTPMonitor], sftpSource, paser), name = "Listener")
        listener ! Opt.START
        val positionServer = context.actorOf(Props.create(classOf[SftpPositionServer]), name = "SftpPositionServer")
        positionServer ! Opt.GET

      case (ProcessModel.MONITOR, Status.RUNNING) =>
        println("monitor | running")

      case (Opt.GET, key: String) =>
        numLine = numLine+1
        println("file content = "+key)

      case ("end", line: Int) =>
        assert( numLine == line )

      case msg =>
        println("SftpMonitorActor receive message: "+msg)
    }
  }

  class SftpPositionServer extends Actor {

    def receive = {
      case Opt.GET =>
        val line = 10
        for( x <- 1 to line ) {
          sender() ! (Opt.GET, "第"+x+"行内容")
        }
        sender() ! ("end", line)
      case msg =>
        println("PositionServer receive message: "+msg)
    }

  }

  val system = ActorSystem("SftpMonitor")
  val sma = system.actorOf(Props[SftpMonitorActor], name = "SftpMonitorActor")
  sma ! Opt.START
  //system.shutdown()

}
