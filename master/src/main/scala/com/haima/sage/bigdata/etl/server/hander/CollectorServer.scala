package com.haima.sage.bigdata.etl.server.hander

import akka.http.scaladsl.model.StatusCodes
import akka.pattern.ask
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.store._

import scala.util.{Failure, Success}

class CollectorServer extends StoreServer[Collector, String] {

  import context.dispatcher

  lazy val store: CollectorStore = Stores.collectorStore
  private lazy val server = context.actorSelection("/user/server")

  //大部分逻辑转交给server处理
  override def receive: Receive = {
    case (Opt.SYNC, (Opt.UPDATE, Status.STOPPED)) =>
      store.all().foreach(c => store.set(c.copy(status = Option(Status.STOPPED))))

    case (Opt.GET, Some(host: String), Some(port: Int)) =>
      sender() ! store.get(Collector(host = host, port = port))
    case (Opt.SYNC, (Opt.UPDATE, id: String, status: Status.Status)) =>
      store.get(id).foreach(c => {
        status match {
          case Status.STOPPED | Status.UNAVAILABLE =>
            /*当中转服务不可用时,*/
            store.queryByRelayServer(c).foreach(c => store.set(c.copy(status = Option(Status.UNAVAILABLE))))
          case _ =>
        }
        store.set(c.copy(status = Option(status)))
      }
      )
    case (Opt.SYNC, (Opt.DELETE, id: String)) =>
      server ! (Opt.SYNC, (Opt.DELETE, id))
      val rt = store.delete(id)
      if (notBroadcast)
        sender() ! rt

    case (Opt.DELETE, id: String, "daemon") =>
      server ! (Opt.DELETE, id, "daemon")
    case (Opt.SYNC, (Opt.PUT, id: String, daemon: String)) =>
      context.actorSelection("/daemon/user/server") ! (Opt.CHECK, "Call")
      server ! (Opt.SYNC, (Opt.PUT, id, daemon))
      server ! (Opt.GET, "daemons")
    case (Opt.START, id: String) =>
      val s = sender()
      (server ? (Opt.START, id)).onComplete {
        case Success(rt@BuildResult("200", "928", _)) =>
          s ! rt
        case _ =>
          s ! BuildResult(StatusCodes.NotFound.intValue.toString, "805")
      }
    case (start: Int, limit: Int, orderBy: Option[String@unchecked], order: Option[String@unchecked], sample: Option[Collector@unchecked]) =>
      logger.debug(s"query:$sample")

      val data = store.queryByPage(start, limit, orderBy, order, sample)

      sender() ! data //TODO .copy(result = data.result.map(fileCollectorStatus))

    /*case (Opt.SYNC, (Opt.UPDATE, id: String, status: Status.Status)) =>
      store.get(id).foreach(c => store.set(c.copy(status = Option(status.toString))))*/

    case (Opt.STOP, id: String) =>
      val data: Collector = store.get(id).get
      val s = sender()
      data.status match {
        case Some(Status.RUNNING) =>
          self ! (Opt.SYNC, (Opt.UPDATE, id, Status.STOPPING))
          // 在停掉worker之前停掉与该worker绑定的所有的数据通道
          // 否则会丢失测量数据
          val masterActor = context.actorSelection("/user/server")
          val configs = Stores.configStore.all().filter(_.collector.contains(id)).map(toConfig)
          configs.foreach(config => {
            if (isStart(config.id)) {
              masterActor ! (id, (config, Opt.STOP))
            }
          })
          //转交给sever处理
          (server ? (Opt.STOP, id)).onComplete {
            case Success(rt@BuildResult("200", "928", msg)) => {
              server ? (data.id, Opt.CHECK)
            }.onComplete {
              case Success(d: Status.Status) =>
                logger.info(s"Check success, status is $d")
                store.set(data.copy(status = Some(Status.STOPPING)))
              case _ =>
                logger.info(s"Check failed")
                store.set(data.copy(status = Some(Status.STOPPED)))
            }
              //s ! BuildResult("200","928",msg)
              s ! rt
            case Success(rt: BuildResult) =>
              s ! rt
            case Success(data) =>
              //s ! BuildResult(rt.asInstanceOf[Result].status,"901",rt.asInstanceOf[Result].message)
              s ! BuildResult("503", "901", "error:" + data)
            case Failure(e) =>
              //s ! Result("503", e.getMessage)
              s ! BuildResult("503", "901", e.getMessage)
            case _ =>
              s ! BuildResult(StatusCodes.NotFound.intValue.toString, "805", data.name)
          }
        case Some(Status.UNAVAILABLE) =>
          s ! BuildResult(StatusCodes.NotFound.intValue.toString, "806", data.name)
        //sender() ! Result(StatusCodes.NotFound.intValue.toString, s"${StatusCodes.NotFound.reason} :daemon for COLLECTOR[$id] HAS STOP")
        case _ =>
          s ! BuildResult(StatusCodes.NotFound.intValue.toString, "805", data.name)
        //sender() ! Result(StatusCodes.NotFound.intValue.toString, s"${StatusCodes.NotFound.reason} :daemon for COLLECTOR[$id] NOT FOUND")
      }


    case obj =>
      super.receive(obj)
  }

  /**
    *
    * @param configId
    * @return
    */
  private def isStart(configId: String): Boolean = {
    statusStore.monitorStatus(configId) match {
      case Some(Status.RUNNING) =>
        true
      case _ => false
    }
  }


}