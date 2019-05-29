package com.haima.sage.bigdata.etl.server.worker

import akka.actor.{Actor, Props}
import com.haima.sage.bigdata.etl.common.Constants._
import com.haima.sage.bigdata.etl.common.model.{Opt, ReadPosition}
import com.haima.sage.bigdata.etl.store.Stores
import com.haima.sage.bigdata.etl.store.position.ReadPositionStore
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by zhhuiyan on 15/1/29.
  */


class PositionService extends Actor {
  protected val logger: Logger = LoggerFactory.getLogger(classOf[PositionService])
  private[PositionService] val store: ReadPositionStore = Stores.positionStore


  override def preStart(): Unit = {
    val cache = CONF.getInt(STORE_POSITION_OFFSET) match {
      case 0 =>
        STORE_POSITION_OFFSET_DEFAULT
      case obj =>
        obj
    }
    store.setCache(cache)
  }


  override def receive: Actor.Receive = {
    case ("list", key: String) =>
      sender() ! store.list(key)
      context.stop(self)
    case key: String =>
      sender() ! (store.get(key) match {
        case Some(position) =>
          position
        case None =>
          ReadPosition(key, 0, 0);

      })
      context.stop(self)
    case (Opt.PUT, position: ReadPosition) =>
      val result = store.set(position)
      //logger.debug(s"save position[$position]")
      if (!result) {
        logger.debug("save error")
      }
      context.stop(self)
    case (Opt.FLUSH, position: ReadPosition) =>
      synchronized {
        store.set(position)
        store.flush()
      }
      context.stop(self)
    case (Opt.DELETE, key: String) =>
      store.remove(key)
      context.stop(self)
    case (Opt.RESET, key: String) =>
      store.fuzzyRemove(key)
      context.stop(self)
  }
}

class PositionServer extends Actor {
  protected val logger: Logger = LoggerFactory.getLogger(classOf[PositionServer])


  override def receive: Receive = {


    case (Opt.GET, key: String) =>
      context.actorOf(Props(classOf[PositionService])).forward(key)
    case (Opt.GET, "list", key: String) =>
      context.actorOf(Props(classOf[PositionService])).forward(("list", key))

    case (Opt.DELETE, key: String) =>
      context.actorOf(Props(classOf[PositionService])).forward(Opt.DELETE, key)

    case (Opt.RESET, key: String) =>
      context.actorOf(Props(classOf[PositionService])).forward(Opt.RESET, key)

    case (Opt.PUT, position: ReadPosition) =>
      context.actorOf(Props(classOf[PositionService])).forward(Opt.PUT, position)

    case (Opt.FLUSH, position: ReadPosition) =>
      context.actorOf(Props(classOf[PositionService])).forward(Opt.FLUSH, position: ReadPosition)
    case obj =>
      logger.debug(s"unknown msg[$obj] ")
  }
}
