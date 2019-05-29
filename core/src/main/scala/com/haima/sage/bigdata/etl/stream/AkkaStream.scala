package com.haima.sage.bigdata.etl.stream

import java.io.IOException
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Properties, UUID}

import akka.actor.{Actor, ActorIdentity, ActorRef, ActorSystem, Identify, Props, ReceiveTimeout, Terminated}
import com.haima.sage.bigdata.etl.common.Constants._
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{NetSource, Opt, RichMap, Status}

/**
  * Created by zhhuiyan on 2014/11/5.
  */

class AkkaStream(conf: NetSource, system: ActorSystem) extends QueueStream[RichMap](None, conf.timeout) {
  private val props = Props.create(classOf[Server], this)
  val server: ActorRef = system.actorOf(props, s"subscriber-${UUID.randomUUID()}")
  logger.debug(s"pipe Stream: ${server.path}")

  private lazy val running = new AtomicBoolean(true)


  @throws(classOf[IOException])
  override def close() {
    running.set(false)
    server ! Opt.STOP
    super.close()

  }


  class Server() extends Actor {

    val url = s"akka.tcp://worker@${conf.host.getOrElse("127.0.0.1")}:${conf.port}/user/publisher"

    override def preStart(): Unit = identify()


    def active(remote: ActorRef): Receive = {
      case Opt.START =>

        conf.properties match {
          case Some(p: Properties) =>
            logger.debug(s" on start subscribe is ${self.path.name} with[${p.getOrDefault("identifier", "default").toString}]")
            remote ! (Opt.SUBSCRIBE, p.getOrDefault("identifier", "default").toString)
          case _ =>
            logger.debug(s" on start subscribe is ${self.path.name}")
            remote ! (Opt.SUBSCRIBE, "default")
        }
      case Terminated(actor) =>
        context.unwatch(actor)
        context.unbecome()
        identify()
      case Status.DONE =>
        finished()
        context.unbecome()
        context.stop(self)
      case Opt.STOP =>
        logger.info(s"on stop subscribe name is ${self.path.name}")
        remote ! Opt.UN_SUBSCRIBE
      case data: List[RichMap@unchecked] =>
        if (running.get()) {
          /*确认收到消息*/
          sender() ! true
          logger.debug(s"pipe [${self.path}] receive msg:${data.size}")
          data.foreach(msg => queue.put(msg))
        } else {
          remote ! Opt.UN_SUBSCRIBE
        }


      case msg: Map[String@unchecked, Any@unchecked] =>
        if (running.get()) {
          queue.put(msg)
        } else {
          remote ! Opt.UN_SUBSCRIBE
        }

      // logger.debug(s"pipe [${self.path}] receive msg:$msg")

      case _ =>
        logger.info(s"Unrecognized msg .\n for pipe Server,will be discard ")
    }

    def receive: PartialFunction[Any, Unit] = {
      case ActorIdentity(_, Some(actor)) =>
        context.watch(actor)
        context.become(active(actor))

        self ! Opt.START
      case ActorIdentity(path, None) =>
        //表示本地Server端启动,Restful_Server端未启动
        logger.warn(s"publisher[$path] not available try after 10s ")
        //每隔10秒:请求Restful Server是否开启
        import scala.concurrent.duration._
        context.system.scheduler.scheduleOnce(10 seconds) {
          identify()
        }
      case ReceiveTimeout => identify()

      case Opt.STOP =>
        finished()
        context.stop(self)

      case data: List[RichMap@unchecked] =>
        /*确认收到消息*/
        sender() ! true
        logger.info(s"pipe [${self.path}] receive msg:${data.size}")
        data.foreach(msg => queue.put(msg))
      case msg: Map[String@unchecked, Any@unchecked] =>
        logger.debug(s"pipe [${self.path}] receive msg:$msg")
        queue.put(msg)
      case _ =>
        logger.info(s"Unrecognized msg .\n for pipe Server,will be discard ")
    }

    def identify(): Unit = context.actorSelection(url) ! Identify(url)

    override def postStop(): Unit = {
      logger.info(s"pipe stream[${self.path}}] closed  ")
    }
  }

}


