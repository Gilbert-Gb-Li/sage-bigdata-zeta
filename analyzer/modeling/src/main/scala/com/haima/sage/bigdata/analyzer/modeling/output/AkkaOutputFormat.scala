package com.haima.sage.bigdata.analyzer.modeling.output

import java.io.IOException
import java.util.Date

import akka.actor.{ActorIdentity, ActorPath, ActorSystem, Identify, ReceiveTimeout}
import akka.pattern._
import akka.util.Timeout
import com.haima.sage.bigdata.analyzer.utils.ActorSystemFactory
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.configuration.Configuration

import scala.concurrent.Await

/**
  * Created by evan on 17-9-11.
  */
class AkkaOutputFormat(path: ActorPath, cluster: String, properties: Option[Map[String, String]]) extends RichOutputFormat[RichMap] with Logger with Serializable {

  import scala.concurrent.duration._

  implicit val timeout = Timeout(5 minutes)

  private lazy val system: ActorSystem = ActorSystemFactory.get("flink-output")

  private lazy val remote = system.actorSelection(path)


  private val ADD_ANALYZE_TIME: Boolean = getProperty("analyze_time")
  private val ADD_CLUSTER_INFO: Boolean = getProperty("cluster_info")

  private def getProperty(name: String) = properties match {
    case Some(properties) =>
      properties.get(name) match {
        case Some(value) => value.equals("true")
        case None => false
      }
    case None => false
  }

  override def configure(parameters: Configuration): Unit = {
   // super.configure(parameters)
  }

  override def close(): Unit = {
    try {
      logger.debug(s"out put finished or be closed")
      //      remote ! Opt.STOP
      //      system.terminate()
      ActorSystemFactory.close("flink-output")
    } catch {
      case e: Exception =>
        logger.warn("close error:" + e.getMessage)
    }
  }

  override def writeRecord(value: RichMap): Unit = {
    logger.info(s"analysed data[$value]")
    if (value.nonEmpty) {
      var data = value
      if (ADD_ANALYZE_TIME) data = data + ("a@analyze_time" -> new Date())
      if (ADD_CLUSTER_INFO) data = data + ("a@cluster" -> cluster)
      remote ! data
      /*if (!value.contains("a@analyze_time")) {
        remote ! value + ("a@analyze_time" -> new Date())
      } else {
        remote ! value
      }*/
    }
  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {

    Await.result[Any](remote ? Identify(path), timeout.duration) match {
      case ActorIdentity(`path`, Some(actor)) =>
        logger.debug(s"connected with ${actor}")
      case ActorIdentity(`path`, None) =>
        logger.error(s"Remote actor not available: $path")
        close()
        throw new IOException(s"Remote actor not available: $path")
      case ReceiveTimeout =>
        logger.error(s"connecting to ${path} is Timeout after $timeout minutes")
        close()
        throw new IOException(s"connecting to ${path} is Timeout after $timeout minutes")
    }
  }

}
