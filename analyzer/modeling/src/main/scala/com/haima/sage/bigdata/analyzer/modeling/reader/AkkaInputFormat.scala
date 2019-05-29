package com.haima.sage.bigdata.analyzer.modeling.reader

import java.util.concurrent.atomic.AtomicInteger

import com.haima.sage.bigdata.analyzer.utils.ActorSystemFactory
import com.haima.sage.bigdata.etl.common.model.{NetSource, RichMap}
import com.haima.sage.bigdata.etl.stream.AkkaStream
import org.apache.commons.logging.LogFactory
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.io.statistics.BaseStatistics
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.io.{GenericInputSplit, InputSplitAssigner}

import scala.annotation.tailrec

@SerialVersionUID(1L)
class AkkaInputFormat(conf: NetSource) extends InputFormat[RichMap, GenericInputSplit] {

  private val logger = LogFactory.getLog(classOf[AkkaInputFormat])

  @transient
  private var client: AkkaStream = _


  override def createInputSplits(minNumSplits: Int): Array[GenericInputSplit] = {
    /*
    * now only one connector
    * */
    Array(new GenericInputSplit(0, 1))
  }

  override def getInputSplitAssigner(inputSplits: Array[GenericInputSplit]): InputSplitAssigner = {
    new InputSplitAssigner() {
      val i: AtomicInteger = new AtomicInteger(0)

      override def getNextInputSplit(host: String, taskId: Int): GenericInputSplit =
        if (i.getAndDecrement() < 0)
          null
        else {

          inputSplits(0)
        }
    }
  }

  override def getStatistics(cachedStatistics: BaseStatistics): Null = {
    null
  }


  override def configure(conf: Configuration): Unit = {


  }


  override def open(split: GenericInputSplit): Unit = {

    client = new AkkaStream(conf, ActorSystemFactory.get("akka-input"))


  }


  override def nextRecord(reuse: RichMap): RichMap = {
    client.next()
  }

  @tailrec
  override final def reachedEnd(): Boolean = {
    try {
      !client.hasNext
    } catch {
      case e: Exception =>
        logger.warn(s"$e")
        reachedEnd()
    }

  }

  override def close(): Unit = {

    try {
      if (client != null) client.close()

    } finally {
      client = null
    }
  }
}
