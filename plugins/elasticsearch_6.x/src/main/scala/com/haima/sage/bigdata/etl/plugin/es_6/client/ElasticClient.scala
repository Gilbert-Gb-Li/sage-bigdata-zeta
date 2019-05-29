package com.haima.sage.bigdata.etl.plugin.es_6.client

import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.http.HttpHost
import org.elasticsearch.action.admin.cluster.health.{ClusterHealthRequest, ClusterHealthResponse}
import org.elasticsearch.action.admin.indices.create.{CreateIndexRequest, CreateIndexResponse}
import org.elasticsearch.action.admin.indices.get.GetIndexRequest
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.action.search.{SearchRequest, SearchResponse, SearchScrollRequest}
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentBuilder

import scala.util.{Failure, Success, Try}


object ElasticClient extends Logger {

  private var clients: Map[String, ElasticClient] = Map()

  private def getKey(name: String, host_ports: Array[(String, Int)]) = name + "@" + host_ports.map {
    case (host, port) =>
      host + ":" + port
  }.mkString("_")

  /**
    * 创建一个连接池(连接池中默认创建5个客户端链接)
    * 并设置每个客户端链接状态都为0(0代表没有线程在用，1代表有线程在用)
    *
    * @param name
    * @param host_ports
    * @return
    */
  def apply(name: String, host_ports: Array[(String, Int)]): ElasticClient = {
    val key = getKey(name, host_ports)
    if (!clients.contains(key)) {
      synchronized {
        if (!clients.contains(key)) {
          clients = clients + (key -> new ElasticClient(name, host_ports))
        }
      }
    }
    val client = this.getClient(key)
    client.use()
    client
  }

  /**
    * 从连接池中获取一个客户端链接，然后设置当前客户端链接状态为1
    *
    * @param key
    * @return
    */
  private def getClient(key: String): ElasticClient = {
    clients.get(key).orNull

  }

  private[client] def free(name: String, host_ports: Array[(String, Int)]): Unit = {
    clients = clients - getKey(name, host_ports)
  }


}

/**
  * Created by bbtru on 2017/10/20.
  */
class ElasticClient private(name: String, host_ports: Array[(String, Int)]) {

  import org.slf4j.{Logger, LoggerFactory}

  private val logger: Logger = LoggerFactory.getLogger(classOf[ElasticClient])

  private var used = 0


  private[client] def use(): Unit = {
    synchronized {
      used += 1
    }
  }


  private var _client: RestHighLevelClient = _


  private def create(): RestHighLevelClient = {


    new RestHighLevelClient(RestClient.builder(host_ports.map(t => {
      new HttpHost(t._1, t._2)
    }): _*).setMaxRetryTimeoutMillis(60000))

    //    val settings: Settings = Settings.builder().put("cluster.name", name)
    //      .put("client.transport.ping_timeout", "60s").build
    //    val client = new PreBuiltTransportClient(settings);
    //    host_ports.foreach(item => {
    //      val host = InetAddress.getByName(item._1)
    //      client.addTransportAddress(new TransportAddress(host, item._2))
    //    })
    //    client
  }

  /*
  * 获取真正的客户端
  * */
  private def get(): RestHighLevelClient = {

    if (_client == null) {
      synchronized {
        if (_client == null) {
          _client = create()
        }
      }
    }

    _client
  }

  /*
  *
  * 释放资源
  * */
  def free(): Unit = {
    synchronized {
      used -= 1
    }
    if (used == 0) {
      if (_client != null) {
        _client.close()
        _client = null
      }


      ElasticClient.free(name, host_ports)
    }

  }

  private def _health(request: ClusterHealthRequest)(c: RestHighLevelClient): ClusterHealthResponse = {
    c.cluster().health(request, RequestOptions.DEFAULT)

  }

  def health(request: ClusterHealthRequest): Try[ClusterHealthResponse] = {
    exec(_health(request))
  }

  private def _search(req: SearchRequest)(c: RestHighLevelClient): SearchResponse = {
    c.search(req, RequestOptions.DEFAULT)
  }

  /**
    * 搜索数据
    *
    * @param req
    * @return
    */
  def search(req: SearchRequest): Try[SearchResponse] = {
    exec(_search(req))
  }

  private def _scroll(req: SearchScrollRequest)(c: RestHighLevelClient): SearchResponse = {
    c.scroll(req, RequestOptions.DEFAULT)
  }

  /**
    * 滚动获取数据
    *
    * @param id
    * @param keepalive
    * @return
    */
  def scroll(id: String, keepalive: Long): Try[SearchResponse] = {

    exec(_scroll(new SearchScrollRequest(id).scroll(new TimeValue(keepalive))))
  }

  /**
    * 滚动获取数据
    *
    * @param req SearchScrollRequest
    * @return
    */
  def scroll(req: SearchScrollRequest): Try[SearchResponse] = {
    exec(_scroll(req))
  }

  private def exec[T](func: RestHighLevelClient => T): Try[T] = {
    try {
      Success(func(get()))
    } catch {
      case e: java.io.IOException =>
        logger.warn(s"connect with es has error:${e.getMessage}")

        if (_client != null) {
          _client.close()
          _client = null
        }
        Failure(e)
      case e: Throwable =>
        logger.error(s"execute with es has error:,${e.getClass},${e.getMessage}")
        Failure(e)
    }
  }

  private def _createIndex(index: String, `type`: String, settings: Settings, mapping: XContentBuilder)(c: RestHighLevelClient): CreateIndexResponse = {
    c.indices().create(new CreateIndexRequest(index, settings).mapping(`type`, mapping), RequestOptions.DEFAULT)
  }

  /**
    * 创建index
    *
    * @param index
    * @param `type`
    * @param settings
    * @param mapping
    * @return
    */
  def createIndex(index: String, `type`: String, settings: Settings, mapping: XContentBuilder): Try[CreateIndexResponse] = {
    exec(_createIndex(index, `type`, settings, mapping))
  }

  private def _indicesExists(index: String)(c: RestHighLevelClient): Boolean = {
    c.indices().exists(new GetIndexRequest().indices(index), RequestOptions.DEFAULT)
  }

  /**
    * 检查index是否存在
    *
    * @param index
    * @return
    */
  def indicesExists(index: String): Try[Boolean] = {
    exec(_indicesExists(index))
  }

  private def _submit(bulk: BulkRequest)(c: RestHighLevelClient): BulkResponse = {
    c.bulk(bulk, RequestOptions.DEFAULT)
  }

  /**
    * 提交bulk数据
    *
    * @param bulk
    * @return
    */
  def submit(bulk: BulkRequest): Try[BulkResponse] = {
    exec(_submit(bulk))
  }

  /*
  *
  * 立刻释放资源
  * */
  def freeNow(): Unit = {
    ElasticClient.free(name, host_ports)
    if (_client != null) {
      _client.close()
      _client = null
    }
  }

}
