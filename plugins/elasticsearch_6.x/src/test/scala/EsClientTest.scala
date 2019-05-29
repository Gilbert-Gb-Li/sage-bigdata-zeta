import java.net.InetAddress
import java.util
import java.util.Date

import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestHighLevelClient}
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.common.unit.TimeValue

import scala.util.{Failure, Success, Try}

object EsClientTest {
  def main(args: Array[String]): Unit = {
    val settings = Settings.builder()
      .put("cluster.name", "my_es_cluster").build();
    val client = new RestHighLevelClient(RestClient.builder(
      new HttpHost("172.16.209.7", 9200)).setMaxRetryTimeoutMillis(1000))
    import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
    val request = new ClusterHealthRequest().masterNodeTimeout(TimeValue.timeValueSeconds(20))
      .masterNodeTimeout("20s").waitForYellowStatus



    Try(client.cluster().health(request,RequestOptions.DEFAULT)) match {
      case Success(x) =>
        println("success", x)
      case Failure(e) =>
        println("Failure", e)
    }

    var map = new util.HashMap[String, Object]()
    map.put("@timestamp", new Date())
    map.put("name", "xxx" + System.currentTimeMillis())
    client.index(new IndexRequest("data").source(map),RequestOptions.DEFAULT).status();
    println(System.currentTimeMillis())
  }
}
