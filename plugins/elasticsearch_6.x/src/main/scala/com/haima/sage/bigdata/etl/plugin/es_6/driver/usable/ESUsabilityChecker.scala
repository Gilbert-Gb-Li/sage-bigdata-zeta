package com.haima.sage.bigdata.etl.plugin.es_6.driver.usable

import com.haima.sage.bigdata.etl.common.model.{Usability, UsabilityChecker}
import com.haima.sage.bigdata.etl.driver.ElasticSearchMate
import com.haima.sage.bigdata.etl.plugin.es_6.driver.ElasticSearchDriver
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.unit.TimeValue
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
  * Created by zhhuiyan on 2017/4/17.
  */
case class ESUsabilityChecker(mate: ElasticSearchMate) extends UsabilityChecker {

  lazy val logger = LoggerFactory.getLogger(classOf[ESUsabilityChecker])
  val driver = ElasticSearchDriver(mate)
  val msg: String = mate.uri + " error:"

  override def check: Usability = {
    driver.driver() match {
      case Success(client) =>
        import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
        val request = new ClusterHealthRequest().masterNodeTimeout(TimeValue.timeValueSeconds(20))
          .masterNodeTimeout("20s").waitForYellowStatus

        client.health(request) match {
          case Success(_) =>
            client.free()
            Usability()
          case Failure(e) =>
            e.printStackTrace()
            logger.warn(s"connect to elastic fail:" + e.getStackTrace.mkString(","))
            client.free()
            Usability(usable = false, cause = msg + e.getMessage)
        }
      case Failure(e) =>
        logger.warn(s"get elastic drive fail:" + e.getStackTrace.mkString(","))
        Usability(usable = false, cause = msg + e.getMessage)
    }
  }

}
