package com.haima.sage.bigdata.etl.plugin.es_6.writer

import java.util
import java.util.UUID

import akka.actor.SupervisorStrategy.Stop
import akka.actor._
import akka.util.Timeout

import com.haima.sage.bigdata.etl.common.exception.LogWriteException
import com.haima.sage.bigdata.etl.common.model.Opt._
import com.haima.sage.bigdata.etl.common.model.Status._
import com.haima.sage.bigdata.etl.common.model.writer.NameFormatter
import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import com.haima.sage.bigdata.etl.metrics.MeterReport
import com.haima.sage.bigdata.etl.plugin.es_6.client.ElasticClient
import com.haima.sage.bigdata.etl.plugin.es_6.exception.EsLogWriteException
import com.haima.sage.bigdata.etl.plugin.es_6.utils.ScalaXContentBuilder
import com.haima.sage.bigdata.etl.utils.{Mapper, UUIDUtils}
import com.haima.sage.bigdata.etl.writer.{BatchProcess, DefaultWriter}
import org.elasticsearch.ResourceAlreadyExistsException
import org.elasticsearch.action.UnavailableShardsException
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse
import org.elasticsearch.action.bulk.{BulkRequest, BulkRequestBuilder, BulkResponse}
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.common.UUIDs
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.elasticsearch.common.xcontent.{XContentBuilder, XContentFactory}
import org.elasticsearch.indices.IndexCreationException
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.transport.{NodeNotConnectedException, RemoteTransportException}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/**
  * Author:zhhuiyan
  *
  * DateTime:2014/7/29 16:43.
  */


class ElasticSearchWriter(conf: ES6Writer, report: MeterReport) extends DefaultWriter[ES6Writer](conf, report: MeterReport) {
  import context.dispatcher

  private[writer] val CACHES: util.Queue[BulkRequestBuilder] = new util.LinkedList[BulkRequestBuilder]()
  implicit val timeout = Timeout(10 seconds)
  val client = ElasticClient(conf.cluster, conf.hostPorts)
  var inflush = false;
  private final val formatter = NameFormatter(conf.index, conf.persisRef)
  @volatile
  var store: ActorRef = make

  @throws(classOf[Exception])
  override def preStart(): Unit = {
    self ! CONNECTED
  }

  def make: ActorRef = context.actorOf(Props.create(classOf[DataStore], this, this), s"${self.path.name}_data_store_${UUID.randomUUID()}")

  def terminal(): Unit = {
    context.parent ! (self.path.name, ProcessModel.WRITER, STOPPED)
    client.free()
    logger.debug(s"ElasticSearchWriter ${self.path.name} stopped.")
    context.stop(self)
  }

  override val supervisorStrategy: OneForOneStrategy = OneForOneStrategy() {
    case msg@EsLogWriteException(b, e) =>
      logger.warn(s"write es has error save to cache (${b.numberOfActions()})")
      CACHES.add(b)
      Stop
  }
  var task: Cancellable = null

  override def flush(): Unit = {
    if (!inflush) {
      receivedLog = 0
      inflush = true
      logger.debug(s"writer received[$getCached] take ${System.currentTimeMillis() - now} Millis")
      now = System.currentTimeMillis()
      if (store != null) {
        store ! FLUSH
        /*流量控制,当缓存的数据量太大时,通知数据发送端暂停数据发送*/
        if (task != null && !task.isCancelled) {
          if (context.children.size > 10) {
            logger.debug("流量控制,当缓存的数据量太大时,通知数据发送端暂停数据发送")
            context.parent ! Opt.WAITING
          }
          task = context.system.scheduler.schedule(10 second, 10 second)({
            if (context.children.size > 10) {
              context.parent ! Opt.WAITING
            } else {
              if (task != null && !task.isCancelled) {
                context.parent ! (Opt.STOP, Opt.WAITING)
                task.cancel()
              }
            }
          })
        }
      }
      store = make
    }


  }


  override def redo(): Unit = {
    super.redo()
    CACHES.foreach { b =>
      make ! b
    }
    CACHES.clear()
  }


  var now: Long = System.currentTimeMillis()

  private var existIndices: Set[String] = Set()

  private var receivedLog: Long = 0

  @throws(classOf[LogWriteException])
  def write(batch: Long, t: List[RichMap]): Unit = {

    inflush = false
    receivedLog += 1
    val withIndex = t.map(log => {
      val indices = if (log.get("c@error").isEmpty) {
        formatter.format(log).toLowerCase
      } else {
        "parse_error" + "_" + formatter.format(log).toLowerCase
      }
      (indices, log)
    })


    store forward(batch, withIndex)
    if (getCached > 0 && receivedLog % cacheSize == 0) {

      flush()
    }
  }

  var counter = 0

  override def close(): Unit = {
    synchronized {
      if (store != null) {
        store = null
      }
      if (context.children.nonEmpty) {
        context.children.foreach(_ ! CLOSE)
        logger.debug(s"writer waiting 100 seconds until data has been store to elasticsearch ")
        context.system.scheduler.scheduleOnce(100 milliseconds) {
          self ! STOP
        }


      } else {
        terminal()
      }
    }

  }


  class DataStore(real: ElasticSearchWriter) extends Actor with BatchProcess with Mapper {

    override def write(t: RichMap): Unit = {

    }

    @volatile
    private var caches: List[(String, String, Map[String, Any])] = Nil
    @volatile
    private var running: Boolean = true

    @volatile
    private var redo: Boolean = true

    private val logger: Logger = LoggerFactory.getLogger(classOf[DataStore])


    def existOrCreate(index: String): Unit = existOrCreate(index, conf.indexType, conf.numeric_detection, conf.date_detection, conf.enable_size)


    def existOrCreate(index: String, `type`: String, numeric_detection: Boolean, date_detection: Boolean, enable_size: Boolean) {
      if (!existIndices.contains(index)) {

        client.indicesExists(index) match {
          case Success(false) =>
            createIndex(index, `type`, numeric_detection, date_detection, enable_size) match {
              case Success(response: CreateIndexResponse) if response.isAcknowledged =>
              case Failure(e: ResourceAlreadyExistsException) =>
                logger.debug(s"index[$index/${`type`}] Already Exists !")
              case Failure(e: IndexCreationException) =>
                logger.debug(s"index[$index/${`type`}] Already Exists !")
              case Failure(e) =>
                logger.warn(s"unknown exception[$e] for create index[$index] !")
              //TODO
              case obj =>
                //TODO
                logger.warn(s"unknown exception for create index[$index] !")
            }
            existIndices = existIndices + index
          case Success(true) =>
            existIndices = existIndices + index

          case Failure(exception) =>
            //TODO
            logger.error(s"create index has error:$exception")
        }


      }
    }


    private def createIndex(index: String, `type`: String, numeric_detection: Boolean, date_detection: Boolean, enable_size: Boolean): Try[CreateIndexResponse] = {
      //5个主分片和测试环境，减少副本提高速度
      val settings = Settings.builder()
        .put("number_of_shards", conf.number_of_shards)
        .put("number_of_replicas", conf.number_of_replicas)
        .put("mapping.total_fields.limit", 5000).build() //fixed max fields 5000.build()
      val detection = XContentFactory.jsonBuilder().startObject()
        .field("numeric_detection", numeric_detection)
        .field("date_detection", date_detection)


      val mapping: XContentBuilder = if (enable_size) {
        detection.startObject("_size").field("enabled", enable_size).endObject().endObject()
      } else {
        detection.endObject()
      }
      client.createIndex(index, `type`, settings, mapping)
    }

    def flush(): Unit = {

      try {

        val bulk = caches.foldLeft(new BulkRequest()) {
          case (item: BulkRequest, (id: String, index: String, data: Map[String, Any])) =>
            indexRequest(id, index, data, item)
        }
        val num = bulk.numberOfActions()
        if (num > 0) {
          time(doWork(bulk))
        } else {
          context.stop(self)
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
          logger.error("flush error information {}", e.getMessage)
          schedule()
      }
    }


    /* 提交ES 时间 metric 计算 */
    def time(function: => Unit): Unit = {
      //      val context = timer.time()
      function
      //      context.stop()
    }

    var taskFlush: Cancellable = _
    var taskAdd: Cancellable = _


    def doWork(bulk: BulkRequest)() {
      import scala.collection.JavaConversions._

      val num = bulk.numberOfActions()
      try {
        val now = System.currentTimeMillis()
        client.submit(bulk) match {
          case Success(response: BulkResponse) =>
            caches = Nil
            if (!response.hasFailures) {
              report(Option(context.parent.path.name))
              context.stop(self)
              logger.debug(s"submit a bulk(size:$num), take ${System.currentTimeMillis() - now} Millis")
            } else {


              // logger.warn(s"es 6.x save has error :${response.buildFailureMessage},save data to index_$$")
              caches = response.getItems.filter(_.getFailure != null).map {
                dd =>

                  //dd.getFailure.getCause.printStackTrace()
                  val error = dd.getFailure.getCause match {

                    case _: NodeNotConnectedException =>
                      if (connect) {
                        connect = false
                        context.parent ! LOST_CONNECTED
                      }
                      false
                    case e: UnavailableShardsException =>
                      false
                    case e: RemoteTransportException =>
                      //                    logger.error("RemoteTransportException")
                      false
                    case e: EsRejectedExecutionException =>
                      //                    logger.error("EsRejectedExecutionException")
                      false
                    case _ =>
                      true
                  }

                  bulk.requests().get(dd.getItemId) match {
                    case request: IndexRequest =>
                      (request.id(), request.index(), request.`type`(), request.sourceAsMap(), dd.getFailure.getMessage, error)
                    case request: UpdateRequest =>

                      /*
                       *1.script param
                       *2.script params
                       *3.doc
                       * */
                      val log = if (request.script() != null) {
                        val params = request.script().getParams
                        if (params.containsKey("param")) {
                          request.script().getParams.get("param").asInstanceOf[util.Map[String, Object]]
                        } else {
                          params
                        }
                      } else {
                        request.doc().sourceAsMap()
                      }

                      (request.id(),
                        request.index(),
                        request.`type`(),
                        log,
                        dd.getFailure.getMessage, error)

                  }


              }.map(
                s => {
                  if (s._6) {
                    val indexName = if (s._2.endsWith("_error")) {
                      s._2
                    } else {
                      s._2 + "_error"
                    }
                    existOrCreate(indexName, s._3, false, false, false)
                    val data = new util.HashMap[String, String]()
                    s._4.get("raw") match {
                      case v: String if v.trim.length > 0 =>
                        data.put("raw", v)
                      case _ =>
                        data.put("raw", mapper.writeValueAsString(s._4))
                    }
                    data.put("error", s._5)
                    (s._1, indexName, data.toMap)
                  } else {
                    (s._1, s._2, s._4.toMap)
                  }
                }
              ).toList
              self ! REDO
            }
          case _ =>
            schedule()
        }

      } catch {
        case e: java.io.IOException =>

          logger.warn(s"connet with es has error:${e.getMessage}")


        case e: Exception =>
          e.printStackTrace()
          logger.debug(s"submit data to elastic 6.x has error:${e.getMessage}")
          schedule()
      }
    }

    def schedule(delay: FiniteDuration = 10 seconds): Unit = {
      connect = false
      context.parent ! LOST_CONNECTED
      if (taskFlush == null) {
        redo = true
        taskFlush = context.system.scheduler.scheduleOnce(delay, self, REDO)
      }
    }

    def asConnected(): Unit = {
      connect = true
      context.parent ! CONNECTED
      logger.info(" connect to elasticsearch  success!")
    }

    val usable: ClusterHealthRequest = new ClusterHealthRequest().masterNodeTimeout(TimeValue.timeValueSeconds(20))
      .masterNodeTimeout("20s").waitForYellowStatus

    private def isUsable: Boolean = {
      if (!connect) {

        client.health(usable) match {
          case Success(response) =>
            true
          case Failure(e) =>
            false
        }


      } else {
        true
      }
    }

    def reFlush() {
      if (taskFlush != null) {
        taskFlush.cancel()
        taskFlush = null
      }
      if (running) {
        if (isUsable) {
          if (!connect)
            asConnected()
          flush()
        } else {
          schedule()
        }

      }
    }

    def getSource(javaMap: util.Map[String, Object]): XContentBuilder = {
      new ScalaXContentBuilder().bytes(javaMap)
    }


    def indexRequest(id: String, indices: String, log: Map[String, Any], bulk: BulkRequest): BulkRequest = {
      import scala.collection.JavaConversions._
      val javaMap: util.Map[String, Object] = log.asInstanceOf[Map[String, AnyRef]]
      existOrCreate(indices)
      val _type = log.get("category") match {
        case Some(_t: String) =>
          _t
        case _ =>
          conf.indexType
      }

      /*is  nested */
      conf.asChild match {
        case Some(filed) if filed != null && filed.trim.length > 0 =>
          /* nested */
          conf.parentField match {
            case Some(parent) if parent != null =>
              log.get(parent) match {
                case Some(v) if v != null =>
                  val list: util.List[util.Map[String, Object]] = new util.ArrayList[util.Map[String, Object]]()
                  list.add(javaMap)
                  val inMap: util.Map[String, Object] = Map(filed -> list)
                  val indexRequest = new IndexRequest(indices, _type, v.toString).source(inMap)
                  val script = generateId(log, conf.idFields) match {
                    case Some(id: String) if id.trim.length > 0 =>

                      s"""if(ctx._source.$filed == null || ctx._source.$filed.length==0){
                         |        ctx._source.$filed = list;
                         |     }else{
                         |        boolean isUpdate=false;
                         |        for(int i=0;i< ctx._source.$filed.size();i++){
                         |          if(ctx._source.$filed[i].${conf.idFields.get}=="$id"){
                         |            isUpdate=true;
                         |            ctx._source.$filed[i]=param;
                         |            break;
                         |          }
                         |       }
                         |       if(!isUpdate){
                         |          ctx._source.$filed += param;
                         |       }
                         |}""".stripMargin
                    case _ =>

                      s"""if(ctx._source.$filed == null || ctx._source.$filed.length==0){
                         |   ctx._source.$filed = list;
                         |}else{
                         |   ctx._source.$filed += param;
                         |}""".stripMargin
                  }

                  // println(script)

                  val data = new UpdateRequest(indices, _type, v.toString)
                    .script(new Script(ScriptType.INLINE, "painless", script, Map("param" -> javaMap, "list" -> list)))
                    .upsert(indexRequest)
                    .retryOnConflict(100)
                  bulk.add(data)
                case _ =>



                  val data = new IndexRequest(indices + "_parent_miss", _type, id).source(getSource(javaMap))
                  logger.warn(s"ignore data[$javaMap] no parent find for child array")
                  bulk.add(data)
              }
            case _ =>

              val data = new IndexRequest(indices + "_parent_miss", _type, id).source(getSource(javaMap))
              logger.warn(s"ignore data[$javaMap] no parent field find for child array")
              bulk.add(data)
          }
        case _ =>
          generateId(log, conf.idFields) match {
            case Some(id: String) if id.trim.length > 0 =>
              conf.script match {
                /* execute upsert for count+=1 and so on*/
                case Some(script) if script != null && conf.isScript.getOrElse(false) =>
                  val indexRequest = new IndexRequest(indices, _type, id.toString).source(getSource(javaMap))


                  //                    try {
                  //
                  //                  } catch {
                  //                    case e: ClassCastException =>
                  //                      logger.error(s"index data has error:" + e.getMessage)
                  //                      new IndexRequest(indices + "_mapping_error", _type, id.toString).source((log + ("error" -> e.getMessage)).asInstanceOf[Map[String, AnyRef]].seq)
                  //                    case e: Exception =>
                  //                      logger.error(s"index data has error:" + e.getMessage)
                  //                      new IndexRequest(indices + "_index_error", _type, id.toString).source((log + ("error" -> e.getMessage)).asInstanceOf[Map[String, AnyRef]].seq)
                  //
                  //                  }

                  val data = new UpdateRequest(indices, _type, id)
                    .script(new Script(ScriptType.INLINE, "painless", script, javaMap))
                    .upsert(indexRequest)
                    .retryOnConflict(100)
                  bulk.add(data)
                case _ =>
                  var data = new IndexRequest(indices, _type, id.toString).source(getSource(javaMap))


                  //                    try {
                  //
                  //                  } catch {
                  //                    case e: ClassCastException =>
                  //                      logger.error(s"index data has error:" + e.getMessage)
                  //                      new IndexRequest(indices + "_mapping_error", _type, id.toString).source((log + ("error" -> e.getMessage)).asInstanceOf[Map[String, AnyRef]].seq)
                  //                    case e: Exception =>
                  //                      logger.error(s"index data has error:" + e.getMessage)
                  //                      new IndexRequest(indices + "_index_error", _type, id.toString).source((log + ("error" -> e.getMessage)).asInstanceOf[Map[String, AnyRef]].seq)
                  //
                  //                  }

                  var updateRequest = new UpdateRequest(indices, _type, id).doc(getSource(javaMap))
                  conf.routingField match {
                    case Some(routing) if routing != null =>
                      log.get(routing) match {
                        case Some(v) if v != null =>
                          data = data.routing(v.toString)
                          updateRequest = updateRequest.routing(v.toString)
                        case _ =>
                      }


                    case _ =>
                  }

                  conf.parentField match {
                    case Some(parent) if parent != null =>
                      log.get(parent) match {
                        case Some(v) if v != null =>
                          data = data.parent(v.toString)
                          updateRequest = updateRequest.parent(v.toString)
                        case _ =>
                      }
                    case _ =>
                  }
                  updateRequest = updateRequest.upsert(data).retryOnConflict(100)
                  bulk.add(updateRequest)
              }


            case _ =>
              var data = new IndexRequest(indices, _type, id).source(getSource(javaMap))
              conf.routingField match {
                case Some(routing) if routing != null =>
                  log.get(routing) match {
                    case Some(v) if v != null =>
                      data = data.routing(v.toString)
                    case _ =>
                  }
                case _ =>
              }

              conf.parentField match {
                case Some(parent) if parent != null =>
                  log.get(parent) match {
                    case Some(v) if v != null =>
                      data = data.parent(v.toString)
                    case _ =>
                  }
                case _ =>
              }
              bulk.add(data)
          }
      }
    }

    def generateId(log: Map[String, Any], idFields: Option[String]): Option[String] = {
      conf.idFields match {
        case Some(__ids: String) =>
          val _ids = __ids.split(",")
          if (_ids.nonEmpty) {
            if (_ids.length == 1) {
              log.get(_ids(0)) match {
                case Some(d) if d != null =>
                  Some(d.toString)
                case _ =>
                  None
              }
            } else {
              val ids = _ids.map(id =>
                log.get(id) match {
                  case Some(null) =>
                    null
                  case Some(d) =>
                    d.toString
                  case _ =>
                    null
                }).filter(_ != null).mkString("-")

              if (ids == "") {
                None
              } else {
                Some(UUIDUtils.id(ids))
              }
            }
          } else {
            None
          }
        case _ => None
      }
    }

    override def receive: Actor.Receive = {
      case CLOSE =>
        flush()
        running = false
      case REDO =>
        reFlush()
      case FLUSH =>
        flush()
      case (batch: Long, withIndex: List[(String, RichMap)@unchecked]) =>
        //魔法代码, process会获取send，所以在收到数据时立即执行。
        process(batch)

        withIndex.foreach {
          case (indices: String, log: RichMap) =>
            caches = (UUIDs.base64UUID(), indices, log) :: caches

        }

      /*case log: IndexRequestBuilder =>
        val bulk = client.get().prepareBulk
        bulk.add(log)
      case bulk: BulkRequestBuilder =>
        flush()*/
      case msg =>
        logger.warn(s"cache: unknown message:$msg")
    }

    override def tail(num: Int): Unit = {
      real.tail(num)
    }
  }

}


