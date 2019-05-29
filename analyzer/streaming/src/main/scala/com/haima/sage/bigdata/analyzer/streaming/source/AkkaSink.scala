package com.haima.sage.bigdata.analyzer.streaming.source

import java.io.IOException

import akka.actor.{ActorIdentity, ActorPath, ActorSystem, Identify, ReceiveTimeout}
import akka.pattern._
import akka.util.Timeout
import com.haima.sage.bigdata.analyzer.utils.ActorSystemFactory
import com.haima.sage.bigdata.etl.common.model.{MetricPhase, RichMap}
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.flink.api.common.functions.StoppableFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction

import scala.concurrent.Await

/**
  * Created by zhhuiyan on 2017/5/15.
  */
class AkkaSink(path: ActorPath) extends RichSinkFunction[RichMap] with StoppableFunction with Logger with Serializable {
  private lazy val system: ActorSystem = ActorSystemFactory.get("flink-sink")

  private lazy val remote = system.actorSelection(path)

  import scala.concurrent.duration._

  implicit val timeout = Timeout(5 minutes)

  override def open(parameters: Configuration): Unit = {
    logger.info(s"send flink-sink to " + remote.toString())

    Await.result[Any](remote ? Identify(path), timeout.duration) match {
      case ActorIdentity(`path`, Some(actor)) =>
        logger.debug(s"connected with ${actor}")
        remote ! "flink-sink"
      case ActorIdentity(`path`, None) =>
        logger.error(s"Remote actor not available: $path")
        close()
        throw new IOException(s"Remote actor not available: $path")
      case ReceiveTimeout =>
        logger.error(s"connecting to ${path} is Timeout after $timeout minutes")
        close()
        throw new IOException(s"connecting to ${path} is Timeout after $timeout minutes")
    }

  }

  //8个小时的毫秒数
  override def invoke(value: RichMap): Unit = {
    //logger.info(s"send data to $path  analysed data[$value]")
    if (value.nonEmpty) {

      remote ! RichMap(value)
      remote ! (MetricPhase.FLINKOUT, 1)
      //remote ! value

    }
  }


  override def stop(): Unit = {
    this.close()

  }

  override def close(): Unit = {

    try {
      //      remote ! ("sink", Opt.CLOSE)
      ActorSystemFactory.close("flink-sink")
    } catch {
      case e: Exception =>
        logger.debug(e.getMessage)
    }
  }
}
