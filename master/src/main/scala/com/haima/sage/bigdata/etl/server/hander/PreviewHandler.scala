package com.haima.sage.bigdata.etl.server.hander

import akka.actor.{Actor, ActorRef}
import akka.pattern.ask
import akka.util.Timeout
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.filter.{MapCase, MapRule, ReParser, SwitchRule}
import com.haima.sage.bigdata.etl.common.model.{Collector, Opt, ParserProperties, ParserWrapper}
import com.haima.sage.bigdata.etl.lexer.Flat.{flatMap, toProperties}
import com.haima.sage.bigdata.etl.store.Stores
import org.slf4j.Logger

import scala.annotation.tailrec
import scala.util.{Failure, Success}

trait PreviewHandler extends Actor {

  import Constants.executor

  import scala.collection.JavaConversions._


  implicit lazy val previewTimeout = Timeout(Constants.getApiServerConf(Constants.MASTER_SERVICE_TIMEOUT).toLong, java.util.concurrent.TimeUnit.SECONDS)

  def logger: Logger

  protected def getPath(serverInfo: Collector, path: String = "/user/server") = s"akka.tcp://${serverInfo.system}@${serverInfo.host}:${serverInfo.port}$path"

  def preview(_sender: ActorRef, collector: Collector, msg: AnyRef, wrapper: ParserWrapper = null): Unit = {
    val remote = context.actorSelection(getPath(collector))

    remote.?(Opt.PREVIEW, msg)(previewTimeout).onComplete {
      case Success(Nil) =>
        _sender ! (List(Map("warn" -> s"connected with source but no data return.")), List())
        remote ! (Opt.PREVIEW, Opt.STOP)
      case Success(cache: List[Map[String, Any]@unchecked]) =>

        _sender ! (toConvert(cache.map(t => flatMap(t).toMap)), if (wrapper == null) {
          List()
        } else {
          dataToProperties(cache, wrapper)
        })
      case Failure(e: Throwable) =>
        _sender ! (List(Map("error" -> e.getMessage)), List())
        remote ! (Opt.PREVIEW, Opt.STOP)
      case Success(e) =>
        _sender ! (List(Map("error" -> e)), List())
        remote ! (Opt.PREVIEW, Opt.STOP)
    }
  }

  /*字段对齐*/
  def toConvert(data: List[Map[String, Any]]): List[Map[String, Any]] = {
    val keys: Set[String] = Set(data.flatMap(x => x.keySet): _*)
    data.map(t => {
      t ++ (keys &~ t.keySet).map(x => (x, None)).toMap
    })
  }

  import com.haima.sage.bigdata.etl.utils.ParserPropertiesUtils._

  def dataToProperties(data: List[Map[String, Any]], parser: ParserWrapper): List[ParserProperties] = {
    /*val analyzer = ReParserProcessor(ReParser(parser = parser))*/
    /*data.map(line => trans(analyzer.parse(Map("raw" -> line)).filterNot(_._1.startsWith("_"))))*/
    /*
    * FIXME 预览不出现RAW字段,
    * */
    val _new = data.map(d => toProperties(d.filterNot(_._1.startsWith("_")))).fold(parser.properties.getOrElse(List()))(merge)
    val old = fromRule(ReParser(parser = parser.parser.orNull, ref = parser.id))
    mergeWithType(_new, old).filter(!_.key.contains("c@"))
  }


  @tailrec
  private def parser(rules: List[MapRule], before: List[ParserProperties]): List[ParserProperties] = {
    val empty = List[ParserProperties]()
    rules match {
      case Nil =>
        before
      case head :: tails =>
        head match {
          case ReParser(_, _parser, _ref) =>
            val self = _ref match {
              case Some(ref) =>
                Stores.parserStore.get(ref) match {
                  case Some(parser) =>
                    parser.properties.getOrElse(empty)
                  case _ =>
                    empty
                }

              case _ =>
                empty
            }
            _parser match {
              case p if p != null && p.filter != null && p.filter.length > 0 =>
                parser(tails ++ p.filter, merge(before, self))
              case _ =>
                parser(tails, merge(before, self))
            }

          case data: SwitchRule[MapRule@unchecked, MapCase@unchecked] =>
            val rules: List[MapRule] = data.default match {
              case Some(r) if data.cases.nonEmpty =>
                data.cases.map(_.rule).toList.+:(r)
              case Some(r) =>
                List(r)
              case _ =>
                List()
            }
            parser(rules ++ tails, before)
          case _ =>
            parser(tails, before)
        }
    }

  }

  def fromRule(rule: MapRule): List[ParserProperties] = {
    parser(List(rule), List[ParserProperties]())
  }

}
