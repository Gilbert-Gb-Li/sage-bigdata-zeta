package com.haima.sage.bigdata.etl.server.master


import akka.actor.{ActorIdentity, ActorRef, DeadLetter, Identify, Props, Terminated}
import akka.remote.{AssociatedEvent, AssociationErrorEvent, DisassociatedEvent, QuarantinedEvent}
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.server.hander.CollectorServer
import com.haima.sage.bigdata.etl.store.{CollectorStore, Stores}
import com.haima.sage.bigdata.etl.utils.Logger

import scala.collection.mutable
import scala.concurrent.duration._

/**
  * Created by zhhuiyan on 2015/6/15.
  */
class Watcher extends akka.actor.Actor with Logger {

  import context.dispatcher

  private val ACTOR_REF_PATH_SUFFIX = "/user/server"

  lazy val store: CollectorStore = Stores.collectorStore

  lazy val task: ActorRef = context.actorOf(Props[Task], "task")
  private lazy val server = context.actorOf(Props[CollectorServer])
  // a map ,the key is the path of the actorRef and the value is the id of the collector_worker which match the actorRef
  private val remotes = scala.collection.mutable.HashMap[String, String]()

  // send identify info to acquire an ActorRef
  private def identifying(serverInfo: Collector) = {
    logger.debug(s"identifying  worker[${getPath(serverInfo)}].")
    context.actorSelection(getPath(serverInfo)) ! Identify(serverInfo.id)
  }

  // send identify info to acquire an ActorRef
  private def identifying(path: String) = {
    logger.debug(s" identifying  worker[$path].")
    context.actorSelection(path) ! Identify(remotes(path))
  }

  // acquire ActorRefs which are stored in the database when first start the collector_master
  private def initialRemoteActorRefs() = {
    Stores.collectorStore.all().foreach {
      server =>
        identifying(server)
    }
  }


  //  initialRemoteActorRefs()

  // get actorRef path from a serverInfo
  private def getPath(serverInfo: Collector) = s"akka.tcp://${serverInfo.system}@${serverInfo.host}:${serverInfo.port}/user/server"

  override def receive: Receive = {
    case ActorIdentity(id, None) =>
      remotes.map {
        case (key, value) =>
          (value, key)
      }.get(id.toString) match {
        case Some(path: String) =>
          context.system.scheduler.scheduleOnce(10 seconds) {
            identifying(path)
          }
        case _ =>
          logger.warn(s"unknown identity[$id]")
      }


    case ActorIdentity(id, Some(actor)) =>
      logger.info(s"start to watch the worker[${actor.path}].")
      context.watch(actor)
      remotes.+=(actor.path.toString -> id.asInstanceOf[String])
     /* TODO  Master.workers.+=(id.asInstanceOf[String] -> actor)*/

      //context.actorSelection(getPath(serverInfo)) ! Identify(serverInfo.id)
      val dics = Stores.dictionaryStore.all()
      if (dics.nonEmpty) {
        actor ! ("dictionary", dics.map(dictionary2Map).toMap)
      }


    case Terminated(actor) =>
      logger.error(s"Detected unreachable : [${actor.path}].")
      if (remotes.contains(actor.path.toString)) {
        val id = remotes(actor.path.toString)
        server ! (Opt.UPDATE, id, Status.STOPPED)
       /* TODO Master.workers.-=(remotes(actor.path.toString))*/
        context.unwatch(actor)
        logger.warn(s"unwatch[${actor.path}] cause terminated.")
        //task ! (Opt.START, actor.path.toString, remotes(actor.path.toString))
        //remotes -= actor.path.toString


        context.system.scheduler.scheduleOnce(10 seconds) {
          identifying(actor.path.toString)
        }
      }
    case (Opt.WATCH, serverInfo: Collector) =>
      logger.info(s"watch worker by id[${serverInfo.id}].")
      identifying(serverInfo)
    /* Master.workers.+=(remoteActorPathMatchId(actorRef.path.toString) -> actorRef)*/
    case (Opt.WATCH, actorRef: ActorRef) =>
      logger.info(s"watch worker[$actorRef]")

      /* TODO Master.workers.+=(remotes.getOrElse(actorRef.path.toString, actorRef.path.toString) -> actorRef)*/
      context.watch(actorRef)
    case (Opt.UNWATCH, actorRef: ActorRef) =>
      logger.info(s"unwatch $actorRef")
    /* TODO  Master.workers -= remotes.getOrElse(actorRef.path.toString, "")*/
      remotes -= actorRef.path.toString

      context.unwatch(actorRef)
    case (Opt.UNWATCH, id: String) =>
      logger.info(s"unwatch worker[$id]")
      server ! (Opt.UPDATE, id, Status.STOPPED)
     /* TODO  Master.workers.get(id) match {
        case Some(actor) =>
          context.unwatch(actor)
          remotes -= actor.path.toString
          Master.workers -= id

        case _ =>
      }*/


    case (Opt.WATCH, actorRefPath: String) =>
      logger.info(s"watch worker by path[$actorRefPath]")
      identifying(actorRefPath)
    case Opt.INITIAL =>
      initialRemoteActorRefs()
    case letter: DeadLetter =>
      logger.debug(s"letter.message = ${letter.message} letter.sender = ${letter.sender}")
    case DisassociatedEvent(local, remote, _) =>
      logger.info(s"Disassociated:${remote.toString + ACTOR_REF_PATH_SUFFIX},local:$local,remote:$remote")
      remotes.get(remote.toString + ACTOR_REF_PATH_SUFFIX) match {
        case Some(id) =>
          val collector = store.get(id)
          logger.info("(Watcher)Worker status is "+collector.get.status + " now.")
          logger.info("(Watcher)Is Running?:"+collector.get.status.equals(Some(Status.RUNNING)))
          if(collector.get.status.equals(Some(Status.STOPPING)) || collector.get.status.equals(Status.STOPPED)){
            logger.info("(Watcher)set STOPPED")
            server ! (Opt.SYNC,(Opt.UPDATE, id, Status.STOPPED))
          }else{
            logger.info("(Watcher)set UNAVAILABLE")
            server ! (Opt.SYNC,(Opt.UPDATE, id, Status.UNAVAILABLE))
          }

         /* TODO  if (Master.workers.contains(id)) {
            Master.workers.get(id) match {
              case Some(actor) =>
                context.unwatch(actor)
                logger.warn(s"unwatch[${actor.path}] cause terminated.")
                task ! (Opt.START, actor.path.toString, remotes(actor.path.toString))
                remotes -= actor.path.toString
                task ! (Opt.START, remote.toString + ACTOR_REF_PATH_SUFFIX, id)
              case _ =>
            }
            Master.workers.-=(id)

          }*/
        case None =>

          logger.debug(s"Second DisassociatedEvent : event.remoteAddress = $remote event.localAddress = $local")
      }
    case AssociatedEvent(local, remote, inbound) =>
      logger.debug(s"AssociatedEvent : remote = $remote local = $local")
    case AssociationErrorEvent(cause, local, remote, inbound, level) =>

      cause.getClass.getName match {
        case "akka.remote.InvalidAssociation" =>
          logger.error(s"AssociationError:The remote system has quarantined this system. No further associations to the remote system are possible until this system is restarted.")
          task ! (Opt.STOP, s"$remote$ACTOR_REF_PATH_SUFFIX")
        case "akka.remote.EndpointAssociationException" =>
          logger.warn(s"AssociationError:$cause")
        case other: String =>
          logger.warn(s"AssociationError:Unknown AssociationErrorEvent Type : $other")
      }
    case QuarantinedEvent(address, uid) =>
      logger.warn(s"Quarantined [address:$address,uid:$uid]")
    case obj =>
      logger.warn(s"unknown event-type:class[${obj.getClass}],data:$obj")

  }

  def convertAsset2Map(asset: Asset): Map[String, String] = {
    val map = mutable.HashMap[String, String]()
    map += ("id" -> asset.id)
    map += ("typeId" -> asset.assetsTypeId)
    map += ("ip" -> asset.ipAddress)
    for (macAddress <- asset.macAddress) map += ("mac" -> macAddress)
    for (responsible <- asset.responsible) map += ("responsible" -> responsible)
    for (mobile <- asset.mobile) map += ("mobile" -> mobile)
    for (desc <- asset.desci) map += ("desc" -> desc)
    map.toMap
  }

  def dictionary2Map(dictionary: Dictionary): (String, Map[String, (String, String)]) = {
    (dictionary.key, dictionary.properties.map {
      props =>
        (props.key, (props.value, props.vtype))
    }.toMap)
  }
}
