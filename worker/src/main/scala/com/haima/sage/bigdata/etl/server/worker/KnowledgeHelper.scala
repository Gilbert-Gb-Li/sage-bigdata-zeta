package com.haima.sage.bigdata.etl.server.worker

import akka.actor.{ActorRef, ActorSystem, Props}
import akka.pattern._
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.model.{Opt, RichMap}
import com.haima.sage.bigdata.etl.knowledge.KnowledgeUser
import com.haima.sage.bigdata.etl.server.Worker
import com.haima.sage.bigdata.etl.server.knowledge.KnowledgeUserActor

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class KnowledgeHelper(val table: String,
                      override val column: String,
                      override val script: Option[String] = None) extends KnowledgeUser {
  implicit val timeout = Timeout(60 seconds)
  lazy val system: ActorSystem = Worker.system
  lazy val store: ActorRef = system.actorOf(Props.create(classOf[KnowledgeUserActor], table, column, script))


  /** 获取需要补足的数据,根据字段的具体值 */
  final override def get(value: Any): Option[Map[String,Any]] = {
    //logger.debug("worker/KnowledgeHelper")
    Await.result({
      store ? (Opt.GET, value)
    }.asInstanceOf[Future[Option[Map[String,Any]]]], Duration.Inf)
  }

  /** 获取需要补足的数据,根据字段的具体值 */
  final override def byScript(event: RichMap): RichMap = {
    Await.result({
      store ? (Opt.GET, event)
    }.asInstanceOf[Future[RichMap]], Duration.Inf)
  }

  override def finalize(): Unit = {
    system.stop(store)
  }

  override def close(): Unit = system.stop(store)

  /** 获取所有数据 */
  override def getAll(): Iterable[Map[String, Any]] = {
    //只有到数据同步完成，才进行数据的知识库补充(需要优化的地方)
    Await.result({
      store ? (Opt.GET, "ALL")
    }.asInstanceOf[Future[mutable.Iterable[Map[String, Any]]]], Duration.Inf)
  }
}
