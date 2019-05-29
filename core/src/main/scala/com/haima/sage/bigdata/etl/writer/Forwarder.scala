package com.haima.sage.bigdata.etl.writer

import java.util.concurrent.LinkedBlockingQueue

import akka.actor._
import akka.pattern._
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.exception.LogWriteException
import com.haima.sage.bigdata.etl.common.model.writer.NameFormatter
import com.haima.sage.bigdata.etl.common.model.{RichMap, Status, _}
import com.haima.sage.bigdata.etl.metrics.MeterReport

import scala.concurrent.duration._

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/29 16:43.
  */
object Forwarder {
  var remotes: Map[String, ActorRef] = Map()

  def apply(path: String): Option[ActorRef] = {
    remotes.get(path)
  }

  def apply(path: String, actor: ActorRef): ActorRef = {
    remotes = remotes.+(path -> actor)
    actor
  }
}

class Forwarder(conf: ForwardWriter, report: MeterReport) extends DefaultWriter[ForwardWriter](conf, report: MeterReport) with BatchProcess {
  implicit val timeout = Timeout(20 seconds)

  import context.dispatcher

  val path = s"akka.tcp://${conf.system.getOrElse("worker")}@${conf.host}:${conf.port}/${conf.app.getOrElse("/user/publisher")}"
  val queue = new LinkedBlockingQueue[RichMap](conf.cache.asInstanceOf[Integer] * 10 + 1)

  private final val formatter = NameFormatter(conf.identifier.getOrElse("default"), conf.persisRef)

  logger.info(s"Forwarder:$path")
  var remote: ActorRef = _
  identifying()

  @throws(classOf[LogWriteException])
  def identifying(): Unit = {
    Forwarder(path) match {
      case Some(actor) =>
        remote = actor
        while (queue.size() > 0) {
          self ! queue.poll()
        }
      case _ =>
        context.actorSelection(path) ! Identify(path)
    }

  }


  override def receive: Receive = {
    case ActorIdentity(`path`, Some(actor)) =>
      logger.info(s"Forwarder:init finished, uri : $path")
      remote = Forwarder(path, actor)
    case ActorIdentity(`path`, None) => logger.warn(s"Remote actor not available: $path")
    case ReceiveTimeout => identifying()
    case Opt.CHECK =>
      if (remote != null) {
        sender() ! Status.CONNECTED
      } else {
        sender() ! Status.CONNECTING
      }
    case any =>
      super.receive(any)

  }


  override def write(t: RichMap): Unit = {
    //    val content = timer.time()

    val data = t + ("identifier" -> formatter.format(t).toString)

    if (remote == null) {
      queue.add(data)
      self ! Status.CONNECTING
      logger.warn(s"your set forward now is invalidate please waiting some time")
      context.parent ! (path: String, ProcessModel.WRITER, Status.ERROR, s"your set forward now is invalidate please waiting some time")
    } else {
      queue.add(data)
      if (!connect)
        self ! Status.CONNECTED
      if (queue.size() ==cacheSize) {
        this.flush()
      }
    }

    //    content.stop()
  }

  override def close(): Unit = {
    flush()
    if (remote != null) {
      remote ! Status.FINISHED
    }
    context.parent ! (self.path.name, ProcessModel.WRITER, Status.STOPPED)
    context.stop(self)
  }

  override def flush(): Unit = {


    val data = queue.toArray.toList
    queue.clear()
    (remote ? data).onComplete(t => {
      report()
      tail(getCached)

    }

    )
  }

  override def redo(): Unit = {
    while (queue.size() > 0) {
      self ! queue.poll()
    }
  }
}