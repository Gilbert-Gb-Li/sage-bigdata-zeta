package com.haima.sage.bigdata.etl.reader

import akka.actor.ActorSystem
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.stream.{TCPStream, UDPStream}

class NetLogReader(conf: NetSource, system: ActorSystem) extends LogReader[Event] with Position {

  val host: String = conf.host.getOrElse("0.0.0.0")
  val port: Int = if (conf.port == 0) 2025 else conf.port

  /* if (conf.protocol == null) {
     throw new NullPointerException("protocol can`t null please set !")
   }*/
  logger.debug(s"net listens:${conf.listens} ")
  override val stream: Stream[Event] = wrap(conf.codec, conf.protocol.map(_.protocol) match {
    case Some(TCP()) =>
      new TCPStream(system, conf.host, conf.port, conf.get("encoding"), if (conf.listens == null) Map() else conf.listens.toMap, conf.timeout)
    case Some(UDP()) =>
      new UDPStream(system, conf.host, conf.port, conf.get("encoding"), if (conf.listens == null) Map() else conf.listens.toMap, conf.timeout)
    case _ =>
      throw new UnsupportedOperationException(conf.protocol.toString)
  })

  def skip(skip: Long): Long = 0

  override def path: String = s"${conf.name}://$host:$port"

  override def position: ReadPosition = ReadPosition(path, 0, 0)
}