package com.haima.sage.bigdata.etl.server.knowledge

import java.util
import java.util.Date

import akka.actor.{Actor, ActorRef, ActorSelection, Terminated}
import com.github.benmanes.caffeine.cache.{CacheLoader, LoadingCache}
import com.haima.sage.bigdata.etl.common.model.{Opt, RichMap, Status}
import com.haima.sage.bigdata.etl.utils.Logger
import javax.script._
import jdk.nashorn.api.scripting.NashornScriptEngineFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain

class KnowledgeUserActor(table: String,
                         column: String,
                         script: Option[String] = None) extends Actor with Logger {

  import scala.collection.JavaConverters._

  lazy val server: ActorSelection = context.actorSelection("/user/server")
  private lazy val flinkModelingCache: ListBuffer[ActorRef] = new ListBuffer[ActorRef]()

  import scala.collection.JavaConversions._

  override def preStart(): Unit = {
    logger.debug(s"KNOWLEDGE[$table] FIRST LOAD!")
    server ! (Opt.LOAD, table, 0)
  }


  override def postStop(): Unit = {
    logger.info("KNOWLEDGE USER SERVER STOPPED!")
    close()
  }

  implicit val ord: Ordering[(Map[String, Any], Int, Long)] = Ordering.by {
    data =>
      (data._2 + 1) * data._3
  }
  lazy val cache: mutable.PriorityQueue[(Map[String, Any], Int, Long)] = new mutable.PriorityQueue[(Map[String, Any], Int, Long)]()

  import com.github.benmanes.caffeine.cache.Caffeine

  private lazy val loadingCache: LoadingCache[String, Map[String, Any]] = {
    Caffeine.newBuilder.maximumSize(1000000).initialCapacity(1000000).build(new CacheLoader[String, Map[String, Any]] {




      override def load(k: String): Map[String, Any] = {
        /*TODO load data remotely
        * */
        Map()
      }
    })
  }

  /** 获取需要补足的数据,根据字段的具体值 */
  def get(value: Any): Option[RichMap] = {

    loadingCache.get(value.toString) match {
      case null =>
        None
      case d =>
        Option(RichMap(d))
    }


  }


  private lazy val exec: (mutable.PriorityQueue[(Map[String, Any], Int, Long)], RichMap) => RichMap = if (column == "scala") {
    val engine = {
      val settings = new Settings()
      settings.usemanifestcp.value = true
      settings.usejavacp.value = true
      new IMain(null, settings)
    }
    try {
      engine.asInstanceOf[Compilable].compile(
        s"""
           |import scala.collection.mutable
           |import com.haima.sage.bigdata.etl.common.model.RichMap
           |val exec:(mutable.PriorityQueue[(Map[String, Any], Int, Long)],RichMap)=>RichMap=(cache,event)=>{
           |${script.getOrElse("event")};
           |}
           |exec
           |""".stripMargin).eval().asInstanceOf[(mutable.PriorityQueue[(Map[String, Any], Int, Long)], RichMap) => RichMap]
    } finally {
      engine.close()
    }

  } else {
    val engine: ScriptEngine = {
      val factory = new ScriptEngineManager().getEngineFactories.find(_.getEngineName == "Oracle Nashorn").get.asInstanceOf[NashornScriptEngineFactory]
      factory.getScriptEngine("-doe", "--global-per-engine")
    }
    val compiled = engine.asInstanceOf[Compilable].compile(
      s"""function exec(cache,event){
         ${script.getOrElse("")};
         |  return event;
         |};
         |exec(event);""".stripMargin)
    val bindings = new SimpleBindings

    (cache, event: RichMap) => {
      val data = {
        val source = new util.HashMap[String, Any]()
        source.putAll(event)
        bindings.put("cache", cache)
        bindings.put("event", source)
        val rt = compiled.eval(bindings).asInstanceOf[java.util.Map[String, Any]].toMap
        bindings.clear()
        rt
      }


      RichMap(data)
    }

  }

  /** 获取需要补足的数据,根据字段的具体值 */
  def byScript(event: RichMap): RichMap = {
    try {
      exec(cache, event)
    } catch {
      case e: Exception =>
        event + ("error" -> s"$column: ${e.getMessage}")
    }


  }

  override def receive: Receive = onMessage(false)

  def close(): Unit = {
    cache.clear()
    if (flinkModelingCache.nonEmpty) flinkModelingCache.foreach(_ ! Status.STOPPED)
    context.stop(self)
  }

  /** 获取所有数据 */
  def getAll(): Iterable[Map[String, Any]] = cache.map(tuple => tuple._1)

  private def onMessage(isReady: Boolean): Receive = {
    case (Opt.GET, "isReady") =>
      // logger.debug(s"${self.path} + isReady:" + isReady)
      sender() ! isReady

    case (Opt.GET, "ALL") =>
      //logger.debug("key:　" + key + ", value:　" + value)
      if (isReady)
        sender() ! getAll()
      else
        self forward ((Opt.GET, "ALL"))
    case (Opt.GET, event: RichMap) =>

      if (isReady)
        sender() ! byScript(event)
      else
        self forward ((Opt.GET, event: RichMap))
    case (Opt.GET, value: Any) =>
      //logger.debug("key:　" + key + ", value:　" + value)
      if (isReady)
        sender() ! get(value)
      else
        self forward ((Opt.GET, value))
    case (data: List[Map[String, Any]@unchecked], loadFinishFlag: Boolean) =>

      logger.debug(s"KNOWLEDGE LOAD ${data.size},isfinish:$loadFinishFlag")
      logger.debug(s"KNOWLEDGE Total ${cache.size}")
      data.foreach(d => {
        while (cache.size > 1000000)
          cache.dequeue()
        this.cache.enqueue((d, 0, new Date().getTime))
      }
      )
      if (loadFinishFlag) {

        //loadingCache.invalidateAll()
        //loadingCache.putAll(cache.map(t => (t._1(column.toLowerCase()).toString, t._1)).toMap.asJava)

        logger.debug(s"update cache with $column $script, $table")
        column match {
          case "scala" =>
          case "js" =>
          case _ =>
            logger.debug(s"update cache with $table")
            loadingCache.invalidateAll()
            loadingCache.putAll(cache.map(t => (t._1(column.toLowerCase()).toString, t._1)).toMap.asJava)

        }
        context.become(onMessage(true))
        server ! (Opt.LOAD, table, Status.FINISHED) //知识库从derby同步加载到worker缓存完成
        logger.debug(s"sync knowledge[$table] success，records ${cache.size} !")
      }
    case (Opt.LOAD, Status.FINISHED) =>
      logger.debug(s"load knowledge[$table] success !")
      context.become(onMessage(true))
      flinkModelingCache.foreach(_ ! getAll())
    case ("KNOWLEDGE", "UPDATE", "RELOAD") => //知识库更新
      cache.clear()
      context.become(onMessage(false))
      logger.debug("Knowledge RElOAD!")
      server ! (Opt.LOAD, table, 0) //知识库重新加载

    //将flink上获取模型数据的actor 的ref 缓存起来
    case (path: Option[String]@unchecked, Status.CONNECTED) =>
      context.watch(sender())
      flinkModelingCache.append(sender())
      if (isReady) {
        sender() ! getAll()
      } else {
        self.forward((path, Status.CONNECTED, ""))
      }

    case t@(_: Option[String]@unchecked, Status.CONNECTED, "") =>
      if (isReady) {
        sender() ! getAll()
      } else {
        self.forward(t)
      }

    case Terminated(actor) =>
      context.unwatch(actor)
      flinkModelingCache.-=(actor)
    case obj =>
      logger.debug(s"unknown message:${obj}")
  }
}