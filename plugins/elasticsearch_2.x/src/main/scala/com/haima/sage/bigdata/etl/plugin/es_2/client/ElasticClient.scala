package com.haima.sage.bigdata.etl.plugin.es_2.client

import java.net.InetAddress

import com.haima.sage.bigdata.etl.utils.{Logger, Mapper}
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress

object ElasticClient extends Logger with Mapper {

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
    if (!clients.contains(key)) clients = clients + (key -> new ElasticClient(name, host_ports))
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
  * Created by bbtru on 2017/10/17.
  */
class ElasticClient private(name: String, host_ports: Array[(String, Int)]) {

  private var used = 0


  private[client] def use(): Unit = {
    used += 1
  }

  private val settings: Settings = Settings.settingsBuilder.put("cluster.name", name)
    .put("client.transport.sniff", false).build

  private var client: TransportClient = create()


  private def create(): TransportClient = {
    host_ports.foldLeft(TransportClient.builder().settings(settings).build()) {
      case (item, (host: String, port: Int)) =>
        item.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port))
    }
  }

  /*
  * 获取真正的客户端
  * */
  def get(): TransportClient = client

  /*
  *
  * 释放资源
  * */
  def free(): Unit = {
    used -= 1
    if (used == 0) {
      client.close()
      ElasticClient.free(name, host_ports)
    }

  }

}
