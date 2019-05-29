package com.haima.sage.bigdata.etl.server.knowledge

import akka.actor.{Actor, ActorIdentity, ActorPath, ActorRef, Identify, ReceiveTimeout, Terminated}
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.base.Lexer
import com.haima.sage.bigdata.etl.common.model.filter.{Filter, MapRule}
import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import com.haima.sage.bigdata.etl.knowledge.KnowledgeLoader
import com.haima.sage.bigdata.etl.utils.lexerFactory
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * 负责读取数据和解析数据，发送到KnowledgeStoreServer
  *
  * @param kid    知识库配置id
  * @param source 数据源
  * @param parser 解析规则
  * @param path   KnowledgeStoreServer的ActorPath
  */
class KnowledgeProcessorServer(kid: String, source: DataSource, parser: Parser[MapRule], path: ActorPath) extends Actor {

  //数据源的loader
  lazy val loader: KnowledgeLoader[_, _] = Class.forName(Constants.CONF.getString("app.knowledge.loader." + source.name)).getConstructor(source.getClass).newInstance(source).asInstanceOf[KnowledgeLoader[_, _]]
  private lazy val lexer: Lexer[String, RichMap] = lexerFactory.instance(parser).orNull //解析规则
  private lazy val filter: Filter = Filter(parser.filter) //过滤规则

  // 建模通道状态监控
  private lazy val watcher = context.actorSelection("/user/watcher")

  lazy val logger: Logger = LoggerFactory.getLogger(classOf[KnowledgeProcessorServer])

  def identify(): Unit = context.actorSelection(path) ! Identify(path)

  override def preStart(): Unit = {

  }

  def active(remote: ActorRef): Receive = {
    case Terminated(actor) =>
      context.unwatch(actor)
      context.unbecome()
      self ! Opt.START
    case Opt.CREATE => //创建表或是更新表
      remote ! Opt.CREATE
    case (Opt.CREATE, Status.SUCCESS) => //表的操作成功
      self ! Opt.GET //获取数据
    case Status.FAIL => //表的操作失败
      watcher ! (kid, ProcessModel.KNOWLEDGE, Status.FAIL)
      remote ! Status.STOPPED
      context.stop(self)
    case Opt.GET => //获取数据，发送给KnowledgeStoreServer

      Try {
        val loaderResult = try {
          loader.byPage(1000)
        } catch {
          case e: Exception =>
            e.printStackTrace()
            throw e
        }

        val data = loaderResult._1.flatMap {
          case _data: Map[String@unchecked, Any@unchecked] =>
            if (parser.filter != null) filter.filter(_data) else List(_data)
          case _data: Event =>
            if (parser.filter != null) filter.filter(lexer.parse(_data.content)) else List(lexer.parse(_data.content))
          case msg =>
            throw new Exception(s"un handed msg[$msg]")
        }

        remote ! (data, loaderResult._2)
      } match {
        case Failure(e) =>
          watcher ! (kid, ProcessModel.KNOWLEDGE, Status.FAIL)
          logger.error(e.getMessage)
          remote ! Status.STOPPED
          context.stop(self)
        case Success(_) =>
      }
    //加载数据（外部数据字典到知识库）成功
    case Status.FINISHED =>
      watcher ! (kid, ProcessModel.KNOWLEDGE, Status.FINISHED)
      context.stop(self)

    case Opt.STOP =>
      remote ! Status.STOPPED
      context.stop(self)
  }

  override def receive: Receive = {
    case Opt.START => //启动
      watcher ! (kid, ProcessModel.KNOWLEDGE, Status.RUNNING)
      identify()
    case ActorIdentity(`path`, Some(actor)) =>
      context.watch(actor)
      context.become(active(actor))
      self ! Opt.CREATE
    case ActorIdentity(`path`, None) =>
      import context.dispatcher
      //表示本地Server端启动,Restful_Server端未启动
      logger.warn(s"daemon[$path] not available")
      //每隔10秒:请求Restful Server是否开启
      context.system.scheduler.scheduleOnce(10 seconds) {
        identify()
      }
    case Opt.STOP =>
      context.stop(self)
    case ReceiveTimeout =>

  }
}
