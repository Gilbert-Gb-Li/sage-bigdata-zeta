package com.haima.sage.bigdata.etl.driver.usable

import com.haima.sage.bigdata.etl.common.model.{ PrometheusSource, Usability, UsabilityChecker}
import com.haima.sage.bigdata.etl.driver.{ PrometheusDriver, PrometheusMate}

import scala.util.{Failure, Success}

/**
  * Created by liyju on 2017/8/24.
  */
case class PrometheusUsabilityChecker(mate:PrometheusMate)  extends UsabilityChecker  {
  val driver = PrometheusDriver(mate)

  def msg: String = ""

  override def check: Usability = driver.driver() match {
    case Success(_) =>
      mate match {
        case source: PrometheusSource =>
          // 验证source 时间步长和采样步长的关系，采样点不能大于11000
          if(source.rangeStep/source.step>=11000){
            Usability(usable = false, cause = msg + s"exceeded maximum resolution of 11,000 points per timeseries.Try decreasing the query resolution (step=XX).".stripMargin)
          }else{
            Usability()
          }
      }
    case Failure(e) =>
      Usability(usable = false, cause = msg + s"can't connect to prometheus server on '${mate.host}:${mate.port} with ${e.getMessage}'.".stripMargin)
  }

}
