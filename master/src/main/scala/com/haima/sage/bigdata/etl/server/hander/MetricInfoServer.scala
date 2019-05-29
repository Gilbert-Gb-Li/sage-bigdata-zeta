package com.haima.sage.bigdata.etl.server.hander

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.{MetricInfo, MetricPhase, MetricType}
import com.haima.sage.bigdata.etl.store.{MetricInfoStore, Store, Stores}

/**
  * Created by zhhuiyan on 2017/5/16.
  */
class MetricInfoServer extends StoreServer[MetricInfo, String] {
  override def store: MetricInfoStore = Stores.metricInfoStore

  override def receive: Receive = {
    case (collectorId: String, configId: String, metricType: Option[String@unchecked], metricPhase: Option[String@unchecked], from: Option[String@unchecked], to: Option[String@unchecked]) =>

      sender() ! store.get(
        configId,
        metricPhase match {
          case Some(mPhase) => Some(MetricPhase.withName(mPhase.toUpperCase))
          case None => None
        },
        metricType match {
          case Some(mType) => Some(MetricType.withName(mType.toUpperCase))
          case None => None
        },
        from, to)
    case obj =>
      super.receive(obj)
  }


}
