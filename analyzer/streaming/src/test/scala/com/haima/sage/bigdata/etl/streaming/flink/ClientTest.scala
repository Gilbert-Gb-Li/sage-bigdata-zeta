package com.haima.sage.bigdata.etl.streaming.flink

import org.apache.flink.client.program.{ClusterClient, ProgramInvocationException, StandaloneClusterClient}
import org.apache.flink.configuration.Configuration
import org.junit.Test

class ClientTest {

  private val client: ClusterClient[_] = try {
    val conf = new Configuration
    conf.setString("jobmanager.rpc.address","zdp01")
    conf.setInteger("jobmanager.rpc.port", 6123)
    val _client = new StandaloneClusterClient(conf)
    _client
  } catch {
    case e: Exception =>
      e.printStackTrace()
      throw new ProgramInvocationException("Cannot establish connection to JobManager: " + e.getMessage, e)
  }
  @Test
  def yarn(): Unit = {
    //client.cancel("asdasdasd")
  }
}
