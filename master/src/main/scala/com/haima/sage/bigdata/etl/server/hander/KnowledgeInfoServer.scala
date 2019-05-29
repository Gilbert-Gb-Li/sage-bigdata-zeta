package com.haima.sage.bigdata.etl.server.hander
import com.haima.sage.bigdata.etl.store.{KnowledgeInfoStore, Stores}

import scala.collection.mutable

/**
  * Created by liyju on 2017/11/20.
  */
class KnowledgeInfoServer extends StoreServer[mutable.Map[String, String], String]{
  lazy val  store: KnowledgeInfoStore = Stores.knowledgeInfoStore
  override def receive: Receive = {
    case (start: Int, limit: Int, orderBy: Option[String@unchecked], order: Option[String@unchecked], sample: Option[String@unchecked]) =>
      logger.debug(s"query:$sample")
      sender() ! store.queryInfoByPage(start, limit, orderBy, order, sample)
    case obj =>
      super.receive(obj)
  }

}
