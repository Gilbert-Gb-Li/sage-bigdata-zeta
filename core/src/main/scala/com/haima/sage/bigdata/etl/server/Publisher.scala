package com.haima.sage.bigdata.etl.server

import akka.actor.{Actor, ActorRef, Terminated}
import akka.pattern._
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.model.{Opt, RichMap, Status}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._

/**
  * Created by zhhuiyan on 15/4/30.
  */
class Publisher() extends Actor {
  implicit val timeout = Timeout(20 seconds)

  protected val logger: Logger = LoggerFactory.getLogger(classOf[Publisher])
  var subscribers: List[(String, ActorRef)] = Nil
  logger.info(s"publisher[${self.path}] started")

  var i = 0

  def receive: PartialFunction[Any, Unit] = {
    case Terminated(actor) =>
      context.unwatch(actor)
      logger.info(s"Terminated subscribe [$actor] ")
      logger.info(s"subs[$subscribers]")
      subscribers = subscribers.filterNot(x => x._2.path.name == sender().path.name)
      logger.info(s"after remove subs[$subscribers]")
    case (Opt.SUBSCRIBE, identifier: String) =>
      logger.debug(s"identifier = $identifier")
      context.watch(sender())
      logger.info(s"add subscribe [${sender().path.name}] ")
      subscribers = (identifier, sender()) :: subscribers
    case Opt.UN_SUBSCRIBE =>

      logger.info(s"remove subscribe [${sender().path.name}] ")
      logger.info(s"subs[$subscribers]")
      subscribers = subscribers.filterNot(x => x._2.path.name == sender().path.name)
      logger.info(s"after remove subs[$subscribers]")
      sender() ! Status.DONE
      context.unwatch(sender())
    case msg: List[RichMap@unchecked] =>
      send(msg: List[RichMap])
    case msg: RichMap =>
      send(msg)
    case Status.FINISHED =>
      logger.debug(s"ignore finished ")
    case msg =>

      logger.warn(s"unknown msg[$msg],sender is $sender")
  }

  def send(msg: RichMap): Unit = {
    subscribers.foreach {
      case (key, subscribe) =>

        if (msg.get("identifier").contains(key)) {
          if (i % 100000 == 0) {
            logger.info(s"$i subscribe[${subscribe.path.toString}] with [$key]  msg:$msg")
          }
          i += 1
          val data = msg - "identifier"
          subscribe ! data
        }
    }
  }

  def send(msg: List[RichMap]): Unit = {
    /*
     *确认收到消息
     *
    */
    sender() ! true
    val data = msg.groupBy(_.getOrElse("identifier", ""))


    subscribers.par.foreach {
      case (key, subscribe) =>
        logger.info(s"$i subscribe[${subscribe.path.toString}] with [$key]  count:${msg.size}")
        i += 1
        data.get(key) match {
          case Some(d) =>
            val data=d.map(_ - "identifier")
            logger.info(s"[$key]  count:${data.size}")
            subscribe ? data
          case None =>
        }
    }
  }

}
