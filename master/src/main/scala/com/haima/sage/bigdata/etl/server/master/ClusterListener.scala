package com.haima.sage.bigdata.etl.server.master

import akka.actor.{Actor, Address, RootActorPath}
import akka.cluster.ClusterEvent._
import akka.cluster.{Cluster, Member}
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.store._
import com.haima.sage.bigdata.etl.store.resolver.ParserStore
import com.haima.sage.bigdata.etl.utils.Logger

import scala.collection.mutable
import scala.concurrent.duration._

class ClusterListener extends Actor with Logger {

  private lazy val configStore: ConfigStore = Stores.configStore
  // private lazy val collectorStore: CollectorStore = Stores.collectorStore
  private lazy val handleStore: AnalyzerStore = Stores.analyzerStore
  private lazy val dataSourceStore: DataSourceStore = Stores.dataSourceStore
  private lazy val parserStore: ParserStore = Stores.parserStore
  private lazy val writeStore: WriteWrapperStore = Stores.writeStore
  private lazy val taskStore: TaskWrapperStore = Stores.taskStore
  //private lazy val dictionaryStore: DictionaryStore = Store.constructor[DictionaryStore](Constants.getApiServerConf(Constants.DICTIONARY_CLASS))
  private lazy val metricInfoStore: MetricInfoStore = Stores.metricInfoStore

  import context.dispatcher

  private lazy val cluster = Cluster(context.system)
  private lazy final val brothers: mutable.Set[Member] = mutable.LinkedHashSet()
  var init = true

  // subscribe to cluster changes, re-subscribe when restart
  override def preStart(): Unit = {
    //#subscribe
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents,
      classOf[MemberEvent], classOf[UnreachableMember])
    //#subscribe
  }

  override def postStop(): Unit = cluster.unsubscribe(self)

  def receive: PartialFunction[Any, Unit] = {
    case MemberUp(member) =>
      logger.info(s"Member roles: ${member.roles}")
      if (member.address != cluster.selfUniqueAddress.address) {
        brothers += member
        logger.info(s"Member${member.address} is Up")
        if (init) {
          sync()
          init = false
        }
      }else{
      //第一次,master启动,重置状态为停止,等待连接
        if(brothers.isEmpty){
          context.actorSelection("/user/collector-server")  ! (Opt.UPDATE, Status.STOPPED)
        }
      }

    case UnreachableMember(member) =>
      brothers -= member
      logger.info(s"Member detected as unreachable: $member")
    case MemberRemoved(member, previousStatus) =>
      brothers -= member
      logger.info(s"Member is Removed: ${member.address} after $previousStatus")
    case obj: MemberEvent => // ignore
      logger.warn(s"ignore MemberEvent:$obj ")


    case "isLocal" => // ignore
      sender() ! (brothers.isEmpty || !brothers.exists(member => {
        member.uniqueAddress.address == sender().path.address
      }))


    case Opt.SYNC =>
      logger.debug(s"sync ???")
    case (Opt.SYNC, _type: String, date: Long) =>
      sync(list(_type), date, _type)
    case (Opt.UPDATE, _type: String, date: Long) =>
      update(list(_type), date, _type)
    case (_: String, data: List[_]) =>
      save(data)
    case ("workers", workers: Iterable[Collector@unchecked]) =>
      logger.debug(s"workers:$workers")
      workers.foreach(path => context.actorSelection("/user/watcher") ! (Opt.SYNC, path))
    case (Opt.SYNC, "workers") =>
      /*
      * TODO
      */
      logger.debug(s"workers:reback")
      context.actorSelection("/user/server") forward "workers"
    /*assetType*/

    case msg => // ignore
      brothers.foreach(brother => {
        val path = remotePath(brother,"/user/server")
        logger.debug(s" self:${self.path} broadcast data[$msg] to :${brother.address}")
        context.actorSelection(path) forward(Opt.SYNC, msg)
      })

  }

  def sync(): Unit = {
    val remote = context.actorSelection(remotePath(brothers.head.address))
    context.system.scheduler.scheduleOnce(1 seconds) {
      remote ! (Opt.SYNC, "metric", last("metric"))
      remote ! (Opt.SYNC, "write", last("write"))
      remote ! (Opt.SYNC, "parser", last("parser"))
      remote ! (Opt.SYNC, "dataSource", last("dataSource"))
      remote ! (Opt.SYNC, "config", last("config"))
      remote ! (Opt.SYNC, "task", last("task"))
      remote ! (Opt.SYNC, "handle", last("handle"))

    }
    logger.debug(s"SYNC:started")
  }


  def last(_type: String): Long = {
    val data = list(_type: String)
    if (data.nonEmpty) {
      data.map(_.lasttime.map(_.getTime).getOrElse[Long](0l)).max[Long]
    } else {
      0l
    }
  }

  def list(_type: String): List[LastAndCreateTime] = {

    _type match {
      case "write" =>
        writeStore.all()
      case "config" =>
        configStore.all()
      case "dataSource" =>
        dataSourceStore.all()
      case "parser" =>
        parserStore.all()
      case "metric" =>
        metricInfoStore.all()
      case "handle" =>
        handleStore.all()
      case "task" =>
        taskStore.all()
    }
  }

  def save(list: List[_]) {
    list.foreach {
      case data: ConfigWrapper =>
        configStore.set(data)
      case data: WriteWrapper =>
        writeStore.set(data)
      case data: DataSourceWrapper =>
        dataSourceStore.set(data)
      case data: ParserWrapper =>
        parserStore.set(data)
      case data: MetricInfo =>
        metricInfoStore.set(data)
      case data: AnalyzerWrapper =>
        handleStore.set(data)

    }

  }

  def sync(list: List[LastAndCreateTime], date: Long, atype: String): Unit = {
    if (list.isEmpty) {
      if (date != 0)
        sender() ! (Opt.UPDATE, atype, 0l)
    } else {
      val last = list.map(_.lasttime.map(_.getTime).getOrElse(0l)).max
      if (last < date) {
        sender() ! (Opt.UPDATE, atype, last)
      } else if (last > date) {
        sender() ! (atype, list.filter(d => d.lasttime.get.getTime > date))
      }
    }
  }

  def update(list: List[LastAndCreateTime], date: Long, `type`: String): Unit = {

    val data = list.filter(item => item.lasttime.get.getTime > date)
    brothers.foreach(brother => {
      val path = remotePath(brother)
      logger.debug(s"self:${self.path} broadcast[${`type`}] to :${brother.address}")
      context.actorSelection(path) ! (`type`, data)
    })
  }

  def remotePath(member: Member, path: String = context.self.path.toStringWithoutAddress): String = {

    RootActorPath(member.address).toString + path
  }

  def remotePath(member: Address): String = {

    RootActorPath(member).toString + context.self.path.toStringWithoutAddress
  }

}