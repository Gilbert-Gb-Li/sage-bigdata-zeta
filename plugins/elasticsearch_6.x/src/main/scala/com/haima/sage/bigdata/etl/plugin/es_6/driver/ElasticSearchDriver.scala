package com.haima.sage.bigdata.etl.plugin.es_6.driver

import com.haima.sage.bigdata.etl.driver.{Driver, ElasticSearchMate}
import com.haima.sage.bigdata.etl.plugin.es_6.client.ElasticClient
import com.haima.sage.bigdata.etl.utils.Logger

import scala.util.Try

/**
  * Created by zhhuiyan on 2017/4/17.
  */
case class ElasticSearchDriver(mate: ElasticSearchMate) extends Driver[ElasticClient] with Logger {

  def driver(): Try[ElasticClient] = Try(
    ElasticClient(mate.cluster, mate.hostPorts)
  )

}
