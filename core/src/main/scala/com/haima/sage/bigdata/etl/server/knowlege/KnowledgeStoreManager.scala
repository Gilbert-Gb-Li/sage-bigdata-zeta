package com.haima.sage.bigdata.etl.server.knowlege

import akka.actor.{Actor, ActorRef, Props}
import com.haima.sage.bigdata.etl.common.model.{RichMap, Status}
import com.haima.sage.bigdata.etl.server.knowledge.KnowledgeStoreServer
import com.haima.sage.bigdata.etl.store.{AnalyzerStore, Stores}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by evan on 18-4-9.
  */
class KnowledgeStoreManager(managerId: String) extends Actor {
  private lazy val logger: Logger = LoggerFactory.getLogger(classOf[KnowledgeStoreManager])
  private var storeCatch: Map[String, ActorRef] = Map()
  private val analyzerStore: AnalyzerStore = Stores.analyzerStore

  def proc(data: RichMap): Unit = {
    data.get("a@id") match {
      case Some(id: String) =>
        if (storeCatch.get(id).isEmpty) {
          analyzerStore.get(id) match {
            case Some(analyzerWrapper) =>
              analyzerWrapper.data match {
                case Some(analyzer) =>
                 // val name = "\"" + s"KNOWLEDGE_${managerId.replace("-", "_").toUpperCase()}@${id.replace("-", "_").toUpperCase()}" + "\""
                  val name = s"KNOWLEDGE_${managerId.replace("-", "_").toUpperCase()}@${id.replace("-", "_").toUpperCase()}"
                  val store: ActorRef = context.actorOf(Props.create(classOf[KnowledgeStoreServer],
                    "CREATE", Some(analyzer.schema(s"$name"))))
                  storeCatch = storeCatch.+(id -> store)
                  storeCatch(id).forward(data.-("a@id"))
                case None =>
                  logger.error(s"Bad analyzer config, $analyzerWrapper")
              }
            case None =>
              logger.error(s"Analyzer(a@id=$id) doesn't exits in system.")
          }
        } else {
          storeCatch(id).forward(data.-("a@id"))
        }
      case _ =>
        logger.error(s"Received data doesn't contain a@id(the id of Analyzer), $data")
    }
  }

  override def receive: Receive = {
    case data: RichMap =>
      proc(data)
    case Status.FINISHED =>
      storeCatch.values.foreach(_.forward(Status.FINISHED))
      context.stop(self)
  }
}
