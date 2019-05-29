package com.haima.sage.bigdata.etl.preview

import java.util.{Date, UUID}

import akka.actor.{ActorRef, Props}
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants.{CONF, MONITORS}
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.Opt.Opt
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.{MapRule, ReParser}
import com.haima.sage.bigdata.etl.common.plugin.Checkable
import com.haima.sage.bigdata.etl.driver.{CheckableServer, FlinkMate}
import com.haima.sage.bigdata.etl.lexer.filter.ReParserProcessor
import com.haima.sage.bigdata.etl.processor.Processor
import org.slf4j.{Logger, LoggerFactory}

class PreviewServer extends CheckableServer {

  protected val logger: Logger = LoggerFactory.getLogger(classOf[Processor])

  private lazy val hostname: String = CONF.getString("akka.remote.netty.tcp.hostname")
  private lazy val port: String = CONF.getString("akka.remote.netty.tcp.port")

  def doResult(sender: ActorRef, isDataSource: Boolean): PartialFunction[Any, Unit] = {
    case (Some(reader: LogReader[_]), Some(parser: Parser[MapRule@unchecked])) =>

      val analyzer = ReParserProcessor(ReParser(parser = parser))
      val result: List[RichMap@unchecked] = try {

        val rt1 = reader.take(5000, 10)._1.map {
          case event: Event =>
            /*header master no need*/
            richMap(Map("c@raw" -> event.content,
              "c@path" -> reader.path,
              "c@collector" -> s"akka.tcp://$hostname:$port",
              "c@receive_time" -> new Date()
            ))
          case event: Map[String@unchecked, Any@unchecked] =>
            richMap(event.+(
              "c@path" -> reader.path,
              "c@collector" -> s"akka.tcp://$hostname:$port",
              "c@receive_time" -> new Date()
            ))
        }

        if (isDataSource) {
          rt1.toList
        } else {
          rt1.flatMap(analyzer.parse).filter(x => x.nonEmpty).toList
        } match {
          case Nil =>
            List(Map("raw" -> "the source is empty "))
          case data =>
            toConvert(data)

        }

      } catch {
        case e: Exception =>
          e.printStackTrace()
          List(Map("raw" -> "the source is error or data formats not match"))
      } finally {
        reader.close()
        context.children.foreach(_ ! Opt.STOP)
      }
      sender ! result.map(vectorFilter)
      context.stop(self)

    case (ProcessModel.MONITOR,Status.UNKNOWN,msg) =>
      context.children.foreach(_ ! Opt.STOP)
      sender ! List(RichMap(Map("error"-> msg)))
      context.stop(self)

    case result: List[RichMap@unchecked] =>
      context.children.foreach(_ ! Opt.STOP)
      sender ! result.map(vectorFilter)
      context.stop(self)

  }


  override def receive: PartialFunction[Any, Unit] = {
    case (channel: Channel, analyzer: Analyzer) =>
      logger.debug(s"preview analyzer[$analyzer]")
      if (FlinkMate("", AnalyzerModel.MODELING, analyzer.name).installed()) {
        val previewer = context.actorOf(Props.create(Class.forName(Constants.CONF.getString(Constants.MODELING_PREVIEWER_CLASS))), s"previewer-${UUID.randomUUID()}")
        previewer ! (channel, analyzer)
        context.become(doResult(sender(), false).orElse(receive))
      } else {
        sender() ! List(Map("raw" -> s"The analyzer server unusable :plugin flink not install"))
      }
    //解析预览
    case (datasource: DataSource, parser: Parser[MapRule@unchecked]) =>
      datasource match {
        case c: Checkable =>
          val usable = checkup(c)
          if (usable.usable) {
            val clazz = Class.forName(MONITORS.getString(datasource.name))
            context.actorOf(Props.create(clazz, datasource, parser).
              withDispatcher("akka.dispatcher.monitor"),
              s"monitor-${datasource.name}-${UUID.randomUUID().toString}") ! Opt.START
            logger.debug(s"start parser preview")
            context.become(doResult(sender(), false).orElse(receive))
          } else {
            sender() ! List(Map("raw" -> s"The source unusable :${usable.cause}"))
          }
        /* if (!sender().path.name.contains("previewer-")) {
           //分析预览时不发送
           sender() ! (Opt.WAITING, self.path.toStringWithoutAddress)
         }*/
      }

    //数据源预览
    case datasource: DataSource =>
      datasource match {
        case c: Checkable =>
          val usable = checkup(c)
          if (usable.usable) {

            val clazz = Class.forName(MONITORS.getString(datasource.name))
            val parser = TransferParser()
            context.actorOf(Props.create(clazz, datasource, parser).
              withDispatcher("akka.dispatcher.monitor"),
              s"monitor-${datasource.name}-${UUID.randomUUID().toString}") ! Opt.START
            logger.debug("start datasource preview")
            context.become(doResult(sender(), true).orElse(receive))
          } else {
            sender() ! List(Map("raw" -> s"The source unusable :${usable.cause}"))
          }
        //  sender() ! (Opt.WAITING, self.path.toStringWithoutAddress)
      }

    case Opt.STOP =>
      context.stop(self)
    case (ProcessModel.MONITOR, act: Opt) =>
      logger.info(s"monitor is $act")
    case msg =>
      logger.warn(s"ignore msg:$msg,sender is:$sender")
  }

  def toConvert(data: List[RichMap]): List[RichMap] = {
    val keys: Set[String] = Set(data.flatMap(x => x.keySet): _*)
    data.map(t => {
      t ++ (keys &~ t.keySet).map(x => (x, None)).toMap
    })
  }

  def vectorFilter(data: RichMap): RichMap = {
    if (data.contains("vector"))
      data + ("vector" -> data.get("vector").get.toString)
    else
      data
  }
}
