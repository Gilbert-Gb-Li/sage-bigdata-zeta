package com.haima.sage.bigdata.etl.utils

import java.io.{BufferedReader, FileReader, UnsupportedEncodingException}
import java.net.{DatagramPacket, InetSocketAddress, Socket}
import java.text.SimpleDateFormat
import java.util.Date
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.ReferenceCountUtil
import org.junit.Test

/**
  * Created by zhhuiyan on 2017/2/10.
  * TODO finished this test
  */
class SocketTest {

  @Test
  def tcpSend(): Unit = {
    val socket = new Socket("172.16.91.231", 1231)
    socket.setTcpNoDelay(true)
    val os = socket.getOutputStream
    val data = "$1,$2,$3\n"
    (0 until 10000).foreach(i => {
     val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      os.write(data.replace("$1", (i % 100).toString) .replace("$2", sdf.format(new Date())).replace("$3", (Math.random() * 10).toInt.toString).toString().getBytes)
      os.flush()
      TimeUnit.MILLISECONDS.sleep(10)
      //socket.getOutputStream.write("\n".getBytes)

    })
    os.close()
    socket.close()
  }

  @Test
  def send(): Unit = {
    val socket = new Socket("127.0.0.1", 1234)
    socket.setTcpNoDelay(true)

    /*val data=s"sub:TDS_MS-SQL_口令弱;se:30;sr:192.168.3.144;sport:0;dest:192.168.3.41;dport:2;proto:u;param:用户名称:sa;用户口令:123456;time:2005-4-19_11:46:35"
    socket.getOutputStream.write(data.getBytes)
    socket.getOutputStream.write("\n".getBytes)*/
    /*(0 to 1000000000).foreach(j => {

    })*/
    val os = socket.getOutputStream
    val sb = new StringBuilder()
    (0 until 10000).foreach(i => {

      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val data = "{\"timestamp\": \"$1\",\"tags1\": {\"appName\": \"quartz-demo-lyf-local-01\",\"appInstanceId\": \"quartz-demo-lyf-local-01-01\",\"space\": \"lyf-local-dev\",\"org\": \"lyf-local\",\"appGuid\": \"quartz-demo-lyf-local-01\",\"zone\": \"sz\"},\"name\": \"quartz\",\"type\": \"gauge\",\"fields\": {\"AllJobDetails\": {\"jobForStatful\": {\"jobGroupName\": {\"shouldRecover\": true,\"durability\": false,\"description\": null,\"name\": \"jobForStatful\",\"group\": \"jobGroupName\",\"jobDataMap\": {\"PRCCOD\": \"QRTZDMO2\",\"EXECUTE_COUNT\": \"15483\"},\"jobClass\": \"com.cmb.framework.ext.scheduler.StatefulJob\"}},\"JobName\": {\"JobGroupName\": {\"shouldRecover\": true,\"durability\": false,\"description\": null,\"name\": \"JobName\",\"group\": \"JobGroupName\",\"jobDataMap\": {\"PRCCOD\": \"QRTZDMO1\"},\"jobClass\": \"com.cmb.framework.ext.scheduler.BaseJob\"}},\"Job2017\": {\"JG2017\": {\"shouldRecover\": true,\"durability\": false,\"description\": null,\"name\": \"Job2017\",\"group\": \"JG2017\",\"jobDataMap\": {\"PRCCOD\": \"QRTZDMO1\"},\"jobClass\": \"com.cmb.framework.ext.scheduler.BaseJob\"}},\"TaskName\": {\"GroupName\": {\"shouldRecover\": true,\"durability\": false,\"description\": null,\"name\": \"TaskName\",\"group\": \"GroupName\",\"jobDataMap\": {\"PRCCOD\": \"QRTZDMO1\"},\"jobClass\": \"com.cmb.framework.ext.scheduler.BaseJob\"}}},\"ThreadPoolSize\": 2,\"SampledStatisticsEnabled\": false,\"Shutdown\": false,\"Started\": true,\"CurrentlyExecutingJobs\": { },\"PerformanceMetrics\": {\"JobsExecuted\": 0,\"JobsScheduled\": 0,\"JobsCompleted\": 0},\"JobsScheduledMostRecentSample\": 0,\"JobsCompletedMostRecentSample\": 0,\"PausedTriggerGroups\": [ ],\"SchedulerInstanceId\": \"6mq974d5aaa1487232024679\",\"StandbyMode\": false,\"JobGroupNames\": [\"JG2017\",\"jobGroupName\",\"GroupName\"],\"TriggerGroupNames\": [\"JG2017\",\"jobGroupName\",\"GroupName\"],\"JobsExecutedMostRecentSample\": 0,\"triggerState\": {\"GroupName#TaskName\": \"NORMAL\",\"JG2017#Job2017\": \"NORMAL\",\"jobGroupName#jobForStatful\": \"NORMAL\",\"JobGroupName#JobName\": \"NORMAL\"},\"AllTriggers\": [{\"jobGroup\": \"JG2017\",\"nextFireTime\": \"2017-02-17T07:00:11Z\",\"finalFireTime\": \"2019-01-24T06:37:11Z\",\"previousFireTime\": \"2017-02-17T06:59:11Z\",\"calendarName\": null,\"endTime\": \"2019-01-24T06:37:14Z\",\"jobName\": \"Job2017\",\"startTime\": \"2017-02-16T09:38:11Z\",\"fireInstanceId\": null,\"priority\": 5,\"description\": null,\"name\": \"Job2017\",\"group\": \"JG2017\",\"misfireInstruction\": 0,\"jobDataMap\": { }},{\"jobGroup\": \"jobGroupName\",\"nextFireTime\": \"2017-02-17T07:00:11Z\",\"finalFireTime\": \"2019-01-24T09:06:11Z\",\"previousFireTime\": \"2017-02-17T06:59:11Z\",\"calendarName\": null,\"endTime\": \"2019-01-24T09:06:51Z\",\"jobName\": \"jobForStatful\",\"startTime\": \"2017-02-16T09:38:11Z\",\"fireInstanceId\": null,\"priority\": 5,\"description\": null,\"name\": \"jobForStatful\",\"group\": \"jobGroupName\",\"misfireInstruction\": 0,\"jobDataMap\": { }},{\"jobGroup\": \"JobGroupName\",\"nextFireTime\": \"2017-02-17T07:01:25Z\",\"finalFireTime\": \"2017-02-20T04:23:25Z\",\"previousFireTime\": \"2017-02-17T06:59:25Z\",\"calendarName\": null,\"endTime\": \"2018-02-21T03:30:15Z\",\"jobName\": \"JobName\",\"startTime\": \"2017-02-16T09:37:25Z\",\"fireInstanceId\": null,\"priority\": 5,\"description\": null,\"name\": \"JobName\",\"group\": \"JobGroupName\",\"misfireInstruction\": 0,\"jobDataMap\": { }},{\"jobGroup\": \"GroupName\",\"nextFireTime\": \"2017-02-17T07:10:48Z\",\"finalFireTime\": \"2017-06-11T05:30:48Z\",\"previousFireTime\": \"2017-02-17T06:50:48Z\",\"calendarName\": null,\"endTime\": \"2017-12-28T03:02:44Z\",\"jobName\": \"TaskName\",\"startTime\": \"2017-02-13T00:30:48Z\",\"fireInstanceId\": null,\"priority\": 5,\"description\": null,\"name\": \"TaskName\",\"group\": \"GroupName\",\"misfireInstruction\": 0,\"jobDataMap\": { }}],\"CalendarNames\": [ ],\"ThreadPoolClassName\": \"com.cmb.framework.ext.scheduler.FrameworkThreadPool\",\"Version\": \"2.2.1\",\"SchedulerName\": \"SomeScheduler\"}}\n"
      sb.setLength(0)
      sb.append(sdf.format(new Date())).append(" ")
      sb.append("quartzMonitor").append(" ")
      sb.append("quartz-demo-lyf-local-01-01").append(" ")
      sb.append("quartz-monitor-01").append(" ")
      sb.append(data.replace("$1", sdf.format(new Date()))).append(" ")
      os.write(sb.toString().getBytes)
      os.flush()
      TimeUnit.MILLISECONDS.sleep(1000)
      //socket.getOutputStream.write("\n".getBytes)

    })
    /* var i = 0
     while (i != -1) {
       val data = Array[Byte](8)
       i = socket.getInputStream.read(data)
      val str=new String(data)
       socket.getOutputStream.write(data)
       if(str.equals("\n")){
         socket.getOutputStream.flush()
       }
     }*/
    socket.close()
  }


  import java.net.InetAddress

  def toDatagram(s: String, destIA: InetAddress, destPort: Int): DatagramPacket = {

    new DatagramPacket(s.getBytes, s.length, destIA, destPort)
  }

  @Test
  def updSend(): Unit = {
    import java.net.{DatagramSocket, InetAddress}
    val server = new DatagramSocket

    val sb = new StringBuilder()
    (0 until 10000).foreach(i => {

      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val data = "{\"timestamp\": \"$1\",\"tags1\": {\"appName\": \"quartz-demo-lyf-local-01\",\"appInstanceId\": \"quartz-demo-lyf-local-01-01\",\"space\": \"lyf-local-dev\",\"org\": \"lyf-local\",\"appGuid\": \"quartz-demo-lyf-local-01\",\"zone\": \"sz\"},\"name\": \"quartz\",\"type\": \"gauge\",\"fields\": {\"AllJobDetails\": {\"jobForStatful\": {\"jobGroupName\": {\"shouldRecover\": true,\"durability\": false,\"description\": null,\"name\": \"jobForStatful\",\"group\": \"jobGroupName\",\"jobDataMap\": {\"PRCCOD\": \"QRTZDMO2\",\"EXECUTE_COUNT\": \"15483\"},\"jobClass\": \"com.cmb.framework.ext.scheduler.StatefulJob\"}},\"JobName\": {\"JobGroupName\": {\"shouldRecover\": true,\"durability\": false,\"description\": null,\"name\": \"JobName\",\"group\": \"JobGroupName\",\"jobDataMap\": {\"PRCCOD\": \"QRTZDMO1\"},\"jobClass\": \"com.cmb.framework.ext.scheduler.BaseJob\"}},\"Job2017\": {\"JG2017\": {\"shouldRecover\": true,\"durability\": false,\"description\": null,\"name\": \"Job2017\",\"group\": \"JG2017\",\"jobDataMap\": {\"PRCCOD\": \"QRTZDMO1\"},\"jobClass\": \"com.cmb.framework.ext.scheduler.BaseJob\"}},\"TaskName\": {\"GroupName\": {\"shouldRecover\": true,\"durability\": false,\"description\": null,\"name\": \"TaskName\",\"group\": \"GroupName\",\"jobDataMap\": {\"PRCCOD\": \"QRTZDMO1\"},\"jobClass\": \"com.cmb.framework.ext.scheduler.BaseJob\"}}},\"ThreadPoolSize\": 2,\"SampledStatisticsEnabled\": false,\"Shutdown\": false,\"Started\": true,\"CurrentlyExecutingJobs\": { },\"PerformanceMetrics\": {\"JobsExecuted\": 0,\"JobsScheduled\": 0,\"JobsCompleted\": 0},\"JobsScheduledMostRecentSample\": 0,\"JobsCompletedMostRecentSample\": 0,\"PausedTriggerGroups\": [ ],\"SchedulerInstanceId\": \"6mq974d5aaa1487232024679\",\"StandbyMode\": false,\"JobGroupNames\": [\"JG2017\",\"jobGroupName\",\"GroupName\"],\"TriggerGroupNames\": [\"JG2017\",\"jobGroupName\",\"GroupName\"],\"JobsExecutedMostRecentSample\": 0,\"triggerState\": {\"GroupName#TaskName\": \"NORMAL\",\"JG2017#Job2017\": \"NORMAL\",\"jobGroupName#jobForStatful\": \"NORMAL\",\"JobGroupName#JobName\": \"NORMAL\"},\"AllTriggers\": [{\"jobGroup\": \"JG2017\",\"nextFireTime\": \"2017-02-17T07:00:11Z\",\"finalFireTime\": \"2019-01-24T06:37:11Z\",\"previousFireTime\": \"2017-02-17T06:59:11Z\",\"calendarName\": null,\"endTime\": \"2019-01-24T06:37:14Z\",\"jobName\": \"Job2017\",\"startTime\": \"2017-02-16T09:38:11Z\",\"fireInstanceId\": null,\"priority\": 5,\"description\": null,\"name\": \"Job2017\",\"group\": \"JG2017\",\"misfireInstruction\": 0,\"jobDataMap\": { }},{\"jobGroup\": \"jobGroupName\",\"nextFireTime\": \"2017-02-17T07:00:11Z\",\"finalFireTime\": \"2019-01-24T09:06:11Z\",\"previousFireTime\": \"2017-02-17T06:59:11Z\",\"calendarName\": null,\"endTime\": \"2019-01-24T09:06:51Z\",\"jobName\": \"jobForStatful\",\"startTime\": \"2017-02-16T09:38:11Z\",\"fireInstanceId\": null,\"priority\": 5,\"description\": null,\"name\": \"jobForStatful\",\"group\": \"jobGroupName\",\"misfireInstruction\": 0,\"jobDataMap\": { }},{\"jobGroup\": \"JobGroupName\",\"nextFireTime\": \"2017-02-17T07:01:25Z\",\"finalFireTime\": \"2017-02-20T04:23:25Z\",\"previousFireTime\": \"2017-02-17T06:59:25Z\",\"calendarName\": null,\"endTime\": \"2018-02-21T03:30:15Z\",\"jobName\": \"JobName\",\"startTime\": \"2017-02-16T09:37:25Z\",\"fireInstanceId\": null,\"priority\": 5,\"description\": null,\"name\": \"JobName\",\"group\": \"JobGroupName\",\"misfireInstruction\": 0,\"jobDataMap\": { }},{\"jobGroup\": \"GroupName\",\"nextFireTime\": \"2017-02-17T07:10:48Z\",\"finalFireTime\": \"2017-06-11T05:30:48Z\",\"previousFireTime\": \"2017-02-17T06:50:48Z\",\"calendarName\": null,\"endTime\": \"2017-12-28T03:02:44Z\",\"jobName\": \"TaskName\",\"startTime\": \"2017-02-13T00:30:48Z\",\"fireInstanceId\": null,\"priority\": 5,\"description\": null,\"name\": \"TaskName\",\"group\": \"GroupName\",\"misfireInstruction\": 0,\"jobDataMap\": { }}],\"CalendarNames\": [ ],\"ThreadPoolClassName\": \"com.cmb.framework.ext.scheduler.FrameworkThreadPool\",\"Version\": \"2.2.1\",\"SchedulerName\": \"SomeScheduler\"}}\n"
      sb.setLength(0)
      sb.append(sdf.format(new Date())).append(" ")
      sb.append("quartzMonitor").append(" ")
      sb.append("quartz-demo-lyf-local-01-01").append(" ")
      sb.append("quartz-monitor-01").append(" ")
      sb.append(data.replace("$1", sdf.format(new Date()))).append(" ")
      // os.write(sb.toString().getBytes)
      server.send(toDatagram(sb.toString(), InetAddress.getByName("127.0.0.1"), 1514))
      // os.flush()
      if (i % 100 == 0)
        println(i)
      TimeUnit.MILLISECONDS.sleep(10)
      //socket.getOutputStream.write("\n".getBytes)

    })

  }

  //@Test
  def fromFileupdSend(): Unit = {
    import java.net.{DatagramSocket, InetAddress}
    val server = new DatagramSocket


    val reader = new BufferedReader(new FileReader("/Users/zhhuiyan/Downloads/demo.txt"))
    var i = 0
    reader.lines.forEach(new Consumer[String] {
      override def accept(line: String): Unit = {
        server.send(toDatagram(line, InetAddress.getByName("127.0.0.1"), 5140))

        if (i % 100 == 0)
          println(i)
        i += 1
        TimeUnit.MILLISECONDS.sleep(1)
        //socket.getOutputStream.write("\n".getBytes)


      }
    })

  }

  // @Test
  def server(): Unit = {

    /* val server = new Server()
     server.start()
    */
    bind(20001)
    Thread.sleep(1000000)

  }

  def bind(port: Int): ChannelFuture = {

    //配置服务端Nio线程组

    try {

      val b = new ServerBootstrap()
      b.group(new NioEventLoopGroup(), new NioEventLoopGroup())
        .channel(classOf[NioServerSocketChannel])
        .option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
        .option[java.lang.Integer](ChannelOption.SO_RCVBUF, 1024)
        .option[java.lang.Integer](ChannelOption.SO_TIMEOUT, 10)
        .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 10)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
        .childHandler(
          new ChannelInitializer[SocketChannel] {
            override def initChannel(ch: SocketChannel): Unit = {
              ch.pipeline().addLast(new TcpServerHandler())
            }
          }).childOption[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
      //绑定端口，同步等待成功
      val f: ChannelFuture = b.bind(port).sync()
      f
    } finally {

      //退出时释放资源


    }

  }

  class TcpServerHandler extends ChannelInboundHandlerAdapter with Logger {
    @throws[UnsupportedEncodingException]
    override def channelRead(ctx: ChannelHandlerContext, msg: Any) {


      try {
        val in: ByteBuf = msg.asInstanceOf[ByteBuf]

        // InetSocketAddress addr = (InetSocketAddress) channel.getRemoteAddress();   Inet4Address net4 = (Inet4Address) addr.getAddress();   String ip = net4.getHostAddress();


        val remote = ctx.channel().remoteAddress().asInstanceOf[InetSocketAddress]
        remote.getHostName
        remote.getHostString
        remote.getPort
        logger.debug("read：" + in.toString())

      } finally {
        ReferenceCountUtil.release(msg)
      }
    }

    override def channelActive(ctx: ChannelHandlerContext) {
      logger.debug(s"tcp server active")

    }

    override def channelInactive(ctx: ChannelHandlerContext) {
      logger.debug(s"tcp server inactive")
      ctx.flush()

    }

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
      logger.error(s"tcp server error  cause by :$cause")
      ctx.close
    }
  }

  class Server extends Thread {
    override def run(): Unit = {
      import java.io._
      import java.net._
      var server: ServerSocket = new ServerSocket(5000)

      var socket: Socket = server.accept()
      socket.setTcpNoDelay(true)

      val is: BufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream));
      var line: String = is.readLine()
      System.out.println("Client:" + line)

      while (line != null) {
        System.out.println("Client:" + line)
        line = is.readLine()

      }
      is.close(); //关闭Socket输入流
      socket.close(); //关闭Socket
      server.close()
    }
  }


}
