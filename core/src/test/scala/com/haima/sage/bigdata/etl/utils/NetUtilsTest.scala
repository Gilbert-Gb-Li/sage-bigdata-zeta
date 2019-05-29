package com.haima.sage.bigdata.etl.utils

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.model.{TCP, UDP}
import com.haima.sage.bigdata.etl.stream.{TCPStream, UDPStream}
import org.junit.{After, Test}

class NetUtilsTest {
  Constants.init("worker.conf")
  final val system = ActorSystem("worker", CONF.resolve())
  val udp = new UDPStream(system, Some("127.0.0.1"), 5140, _timeout = 100)
  val tcp = new TCPStream(system, Some("127.0.0.1"), 5141, _timeout = 100)

  @After
  def clear(): Unit = {
    udp.close()
    //TimeUnit.HOURS.sleep(1)
    tcp.close()
    system.terminate()
  }

  @Test
  def udpTest(): Unit = {
    val toUdp = NetUtils("localhost", 5140, UDP()).connectAble
    val toTcp = NetUtils("localhost", 5140, TCP()).connectAble
    assert(toUdp._1, toUdp._2)
    assert(!toTcp._1, toTcp._2)


  }

  @Test
  def tcpTest(): Unit = {
    val toTcp = NetUtils("127.0.0.1", 5141, TCP()).connectAble
    assert(toTcp._1, toTcp._2)

  }
}
