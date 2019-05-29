package com.haima.sage.bigdata.analyzer.streaming.source

import java.util.concurrent.Executors

import akka.actor.{Actor, ActorIdentity, ActorPath, ActorRef, DeadLetter, Identify, ReceiveTimeout}
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.utils.{ExceptionUtils, Logger}
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

/**
  * Created by zhhuiyan on 2017/5/15.
  */
class SubscribeActor(ctx: SourceFunction.SourceContext[Map[String, Any]],
                     from: Collector,
                     identifier: String,
                     flink: ActorPath,
                     autoAck: Boolean,
                     counter: org.apache.flink.metrics.Counter
                    )
  extends Actor with Logger {

  val path = s"akka.tcp://worker@${from.host}:${from.port}/user/publisher"
  implicit val executor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5))

  def identify(): Unit = {
    context.actorSelection(path) ! Identify("publisher")
  }

  @throws[Exception]
  override def preStart(): Unit = {
    context.actorSelection(path) ! Identify("publisher")
  }

  def identifyFlink(): Unit = {
    context.actorSelection(flink) ! Identify("lexer")
  }

  var count = 0

  def active(remote: ActorRef, lexer: ActorRef): Receive = {
    case letter: DeadLetter =>
      letter.message match {
        case _ =>
          logger.warn(s"from ${letter.sender.toString().split("/").last} ")
          logger.warn(s"to ${letter.recipient.toString().split("/").last}: ")
          logger.warn(s"deadLetter[${letter.message}} ]")
      }
    case Opt.START =>
      logger.info(s"source[${self.path}] started ")

      remote ! (Opt.SUBSCRIBE, identifier)
      lexer ! ("source", Status.RUNNING)
    case Opt.STOP =>
      logger.info(s"source[${self.path}] stopped")
      lexer ! Status.STOPPED
      remote ! Opt.UN_SUBSCRIBE
      context.stop(self)
    case events: List[Map[String@unchecked, AnyRef@unchecked]@unchecked] =>
      count += events.size
      counter.inc(events.size)
      lexer ! (MetricPhase.FLINKIN, events.size)
      logger.info(s"source[${self.path}] received[${events.size}],total:$count")
      /*做计算不涉及真正的数据批次回传处理,表示对于做计算的数据随时丢弃*/

      try {
        events.filter(!_.contains("c@error")).foreach(data => ctx.collect(data))
        sender() ! true
      } catch {
        case e: Exception =>
          logger.error("receive data error", e)
          sender() ! (lexer.path.name, ProcessModel.LEXER, Status.ERROR, s"FLINK RUNTIME ERROR:${ExceptionUtils.getMessage(e)}")
      }
    case data: Map[String@unchecked, AnyRef@unchecked] =>
      ctx.collect(data)
      counter.inc(1)
    case data: Iterable[(String, AnyRef)@unchecked] =>
      collect(data)
    case (data: Map[String@unchecked, AnyRef@unchecked], timestamp: Long) =>
      collect(data, timestamp)
    case msg =>
      logger.warn(s"unknown msg:$msg,sender is [${sender().path}]")

  }


  def withPipe(remote: ActorRef): Receive = {

    case Opt.START =>
      identifyFlink()
    case ActorIdentity("lexer", Some(actor)) =>

      context.become(active(remote, actor))
      self ! Opt.START

    case ActorIdentity("lexer", None) =>
      //表示本地Server端启动,Restful_Server端未启动
      logger.warn(s"publisher[$from] not available try after 10s ")
      //每隔10秒:请求Restful Server是否开启
      import scala.concurrent.duration._
      context.system.scheduler.scheduleOnce(10 seconds) {
        identifyFlink()
      }
    case ReceiveTimeout => identifyFlink()


    // if (autoAck) getSender.tell("ack", getSelf)
  }

  @SuppressWarnings(Array("unchecked"))
  @throws[Exception]
  override def receive: Receive = {

    case ActorIdentity("publisher", Some(actor)) =>

      context.become(withPipe(actor))


      self ! Opt.START

    case ActorIdentity("publisher", None) =>
      //表示本地Server端启动,Restful_Server端未启动
      logger.warn(s"publisher[$from] not available try after 10s ")
      //每隔10秒:请求Restful Server是否开启
      import scala.concurrent.duration._
      context.system.scheduler.scheduleOnce(10 seconds) {
        identify()
      }
    case ReceiveTimeout => identify()

    case msg =>
      logger.warn(s"unknown msg:$msg,sender is [${sender().path}]")
    // if (autoAck) getSender.tell("ack", getSelf)
  }


  /**
    * To handle {@link Iterable} data
    *
    * @param data data received from feeder actor
    */
  private def collect(data: Iterable[(String, AnyRef)]): Unit = {
    counter.inc(1)
    ctx.collect(data.toMap)
  }

  /**
    * To handle single data
    *
    * @param data data received from feeder actor
    */
  private def collect(data: Map[String, Any]) = {
    counter.inc(1)
    ctx.collect(data)
  }

  /**
    * To handle data with timestamp
    *
    * @param data      data received from feeder actor
    * @param timestamp timestamp received from feeder actor
    */
  private def collect(data: Map[String, Any], timestamp: Long) = {
    counter.inc(1)
    ctx.collectWithTimestamp(data, timestamp)
  }

  @throws[Exception]
  override def postStop(): Unit = {
    // dataFrom !
    // remotePublisher.tell(new UnsubscribeReceiver(ActorRef.noSender), ActorRef.noSender)
  }
}
