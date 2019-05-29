package com.haima.sage.bigdata.analyzer.streaming.source

import java.util.concurrent.Executors

import akka.actor.{Actor, ActorIdentity, ActorPath, ActorRef, DeadLetter, Identify, ReceiveTimeout}
import com.haima.sage.bigdata.etl.common.model.{MetricPhase, Opt, ProcessModel, Status}
import com.haima.sage.bigdata.etl.utils.{ExceptionUtils, Logger}
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

/**
  * Created by zhhuiyan on 2017/5/15.
  */
class ReceiverActor(ctx: SourceFunction.SourceContext[Map[String, Any]], from: ActorPath, lexer: ActorPath, autoAck: Boolean)
  extends Actor with Logger {


  implicit val executor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(5))
  // --- Fields set by the constructor

  // --- Runtime fields

  private var WAIT_TIME: Long = 100

  def identify(): Unit = {
    context.actorSelection(from) ! Identify("publisher")
  }

  @throws[Exception]
  override def preStart(): Unit = {
    context.actorSelection(from) ! Identify("publisher")
  }

  def identifyLexer(): Unit = {
    context.actorSelection(lexer) ! Identify("lexer")
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
      lexer ! ("source", Status.RUNNING)
      remote ! Opt.GET
    case Opt.STOP =>
      logger.info(s"source[${self.path}] stopped")
      lexer ! Opt.STOP
      context.stop(self)
    case (Opt.PUT, wait: Long) =>
      logger.info(s"source[${self.path}]  wait[$wait]")
      WAIT_TIME = wait
    case Opt.WAITING =>
      logger.info("no data received")
      context.system.scheduler.scheduleOnce(WAIT_TIME milliseconds, remote, Opt.GET)
    case (Opt.WAITING, Opt.FLUSH) =>
      lexer ! Opt.FLUSH
      context.system.scheduler.scheduleOnce(WAIT_TIME milliseconds, remote, Opt.GET)
    case (_: String, batch: Long, events: List[Map[String@unchecked, AnyRef@unchecked]@unchecked]) =>
      count += events.size
      logger.info(s"source[${self.path}] received[${events.size}],total:$count")
      /*做计算不涉及真正的数据批次回传处理,表示对于做计算的数据随时丢弃*/
      sender() ! ("batch", batch, events.size)
      sender() ! (MetricPhase.FLINKIN, events.size)
      try{
        events.filter(!_.contains("error")).foreach(data => ctx.collect(data))
        sender() ! (lexer.path.name, ProcessModel.LEXER, Status.RUNNING,"")
      }catch {
        case e:Exception=>
          logger.error("receive data error", e)
          sender() ! (lexer.path.name, ProcessModel.LEXER, Status.ERROR, s"FLINK RUNTIME ERROR:${ExceptionUtils.getMessage(e)}")
      }
      sender() ! Opt.GET
    case data: Map[String@unchecked, AnyRef@unchecked] =>
      ctx.collect(data)
    case data: Iterable[(String, AnyRef)@unchecked] =>
      collect(data)
    case (data: Map[String@unchecked, AnyRef@unchecked], timestamp: Long) =>
      collect(data, timestamp)
    case msg =>
      logger.warn(s"unknown msg:$msg,sender is [${sender().path}]")

  }


  def withPipe(remote: ActorRef): Receive = {

    case Opt.START =>
      identifyLexer()
    case ActorIdentity("lexer", Some(actor)) =>

      context.become(active(remote, actor))
      self ! Opt.START

    case ActorIdentity("lexer", None) =>
      //表示本地Server端启动,Restful_Server端未启动
      logger.warn(s"publisher[$from] not available try after 10s ")
      //每隔10秒:请求Restful Server是否开启
      import scala.concurrent.duration._
      context.system.scheduler.scheduleOnce(10 seconds) {
        identifyLexer()
      }
    case ReceiveTimeout => identifyLexer()


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
    ctx.collect(data.toMap)
  }

  /**
    * To handle single data
    *
    * @param data data received from feeder actor
    */
  private def collect(data: Map[String, Any]) = {
    ctx.collect(data)
  }

  /**
    * To handle data with timestamp
    *
    * @param data      data received from feeder actor
    * @param timestamp timestamp received from feeder actor
    */
  private def collect(data: Map[String, Any], timestamp: Long) = {
    ctx.collectWithTimestamp(data, timestamp)
  }

  @throws[Exception]
  override def postStop(): Unit = {
    // dataFrom !
    // remotePublisher.tell(new UnsubscribeReceiver(ActorRef.noSender), ActorRef.noSender)
  }
}
