package com.haima.sage.bigdata.analyzer.streaming.source

import java.lang
import java.util.UUID

import akka.actor.{ActorPath, ActorSystem, Props}
import akka.pattern._
import akka.util.Timeout
import com.haima.sage.bigdata.analyzer.utils.ActorSystemFactory
import com.haima.sage.bigdata.etl.common.model.{Collector, Opt, RichMap}
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.flink.api.common.functions.StoppableFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, _}

/**
  * Created by zhhuiyan on 2017/5/15.
  */
class AkkaSubscribe(path: Collector,
                    identifier: String,
                    lexer: ActorPath,
                    name: String = "")
  extends RichSourceFunction[RichMap] with StoppableFunction with SourceFunction[RichMap] with Logger {

  implicit val timeout = Timeout(10 seconds)
  // --- Runtime fields
  val actorName: String = "flink-" + name + "-" + UUID.randomUUID().toString
  private lazy val system: ActorSystem = ActorSystemFactory.get("flink-source")

  protected lazy val autoAck: lang.Boolean = false


  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    logger.info("flink akka source started")


  }

  @throws[Exception]
  override def run(ctx: SourceFunction.SourceContext[RichMap]): Unit = {

    logger.info("Starting the Receiver actor {}", actorName)
    system.actorOf(Props.create(classOf[SubscribeActor], ctx, path, identifier, lexer, Boolean.box(autoAck), getRuntimeContext.getMetricGroup.counter("numRecordsIn")), actorName)
    logger.info("Started the Receiver actor {} successfully", actorName)

    Await.result(system.whenTerminated, Duration.Inf)
    /*receiverActorSystem.awaitTermination();*/
  }

  override def close(): Unit = {
    logger.info(s"Closing source ${actorName}")
    if (system != null) {
      (system.actorSelection("/user/" + actorName) ? Opt.STOP).onComplete(_ => {
        ActorSystemFactory.close("flink-source")
      })(system.dispatcher)

      //system.awaitTermination(Duration.Zero)

    }
  }

  override def cancel(): Unit = {
    logger.info("Cancelling akka source")
    close()
  }

  override def stop(): Unit = {
    logger.info("Stopping akka source")
    close()
  }
}
