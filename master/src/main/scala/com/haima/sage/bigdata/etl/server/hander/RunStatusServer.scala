package com.haima.sage.bigdata.etl.server.hander

import com.haima.sage.bigdata.etl.common.model.{Opt, ProcessModel, RunStatus, Status}
import com.haima.sage.bigdata.etl.store.{KnowledgeStore, StatusStore, Stores}

/**
  * Created by zhhuiyan on 2017/5/16.
  */
class RunStatusServer extends StoreServer[RunStatus, String] {
  override def store: StatusStore = Stores.statusStore

  override def receive: Receive = {

    case (Opt.SYNC, status: RunStatus) =>
      store.save(status)
      store.init
    case (config: String, ProcessModel.MONITOR) =>
      sender() ! store.monitorStatus(config)
    case config: String =>
      sender() ! store.status(config)
    case Opt.GET =>
      sender() ! store.all
    case (Opt.DELETE, configId: String) =>
      sender() ! store.deleteByConfig(configId)
    case (knowledgeId:String,ProcessModel.KNOWLEDGE ,status: Status.Status )=>
      try {
        if(knowledgeStore.connection ==null)
          throw  new Exception("No current connection")
        val knowledge = knowledgeStore.get(knowledgeId)
        knowledge match {
          case Some(_)=>
            knowledgeStore.set(knowledge.get.copy(status = Some(status.toString)) )
            knowledgeStore.init
          case None=>
            throw  new Exception(s"No knowledge ,id :[$knowledgeId]")
        }
      } catch {
        case e:Exception=>
         e.printStackTrace()
      }

    case obj =>
      super.receive(obj)
  }


}
