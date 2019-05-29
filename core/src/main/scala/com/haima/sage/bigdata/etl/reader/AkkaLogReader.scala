package com.haima.sage.bigdata.etl.reader

import akka.actor.ActorSystem
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.{NetSource, Stream}
import com.haima.sage.bigdata.etl.stream.AkkaStream

class AkkaLogReader(conf: NetSource, system: ActorSystem) extends LogReader[RichMap] {
  val stream: Stream[RichMap] = new AkkaStream(conf, system: ActorSystem)


  def skip(skip: Long): Long = 0

  override val path: String = conf.uri

}