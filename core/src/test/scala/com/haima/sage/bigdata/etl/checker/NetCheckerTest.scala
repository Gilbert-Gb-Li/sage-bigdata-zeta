package com.haima.sage.bigdata.etl.checker

import akka.actor.ActorSystem
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.{Net, NetSource, TCP}
import com.haima.sage.bigdata.etl.stream.{TCPStream, UDPStream}
import org.junit.{After, Test}

class NetCheckerTest {

  Constants.init("worker.conf")
  final val system = ActorSystem("worker", Constants.CONF.resolve())


  @After
  def clear(): Unit = {
    system.terminate()
  }

  @Test
  def netSourceUdpTest(): Unit = {
    val udp = new UDPStream(system, Some("127.0.0.1"), 5140, _timeout = 100)
    val checker = NetChecker(NetSource(host = Some("127.0.0.1"), port = 5140))
    val usable = checker.check

    assert(!usable.usable, usable.cause)
    udp.close()
    val usable2 = checker.check
    assert(usable2.usable, usable2.cause)


  }

  @Test
  def netSourceTcpTest(): Unit = {
    val tcp = new TCPStream(system, Some("127.0.0.1"), 5140, _timeout = 100)
    val checker = NetChecker(NetSource(protocol = Some(Net(protocol = TCP())), host = Some("127.0.0.1"), port = 5140))
    val usable = checker.check
    assert(!usable.usable, usable.cause)
    tcp.close()
    val usable2 = checker.check
    assert(usable2.usable, usable2.cause)

  }

}
