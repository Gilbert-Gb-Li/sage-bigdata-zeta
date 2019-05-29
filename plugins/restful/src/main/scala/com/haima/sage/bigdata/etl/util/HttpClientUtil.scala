package com.haima.sage.bigdata.etl.util


import java.security.cert.{CertificateException, X509Certificate}
import javax.net.ssl.{SSLContext, TrustManager, X509TrustManager}

import org.apache.http.HttpEntityEnclosingRequest
import org.apache.http.NoHttpResponseException
import org.apache.http.client.HttpRequestRetryHandler
import org.apache.http.client.protocol.HttpClientContext
import org.apache.http.conn.ConnectTimeoutException
import javax.net.ssl.SSLException
import javax.net.ssl.SSLHandshakeException
import java.io.IOException
import java.io.InterruptedIOException
import java.net.UnknownHostException

import org.apache.http.HttpStatus
import org.apache.http.client.methods.{CloseableHttpResponse, HttpPost}
import org.apache.http.config.{Registry, RegistryBuilder}
import org.apache.http.conn.socket.{ConnectionSocketFactory, PlainConnectionSocketFactory}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.protocol.HttpContext
import org.apache.http.util.EntityUtils

import scala.util.{Failure, Success, Try}

/**
  * Created by liyju on 2017/8/28.
  */
object HttpClientUtil{
  def apply: HttpClientUtil = new HttpClientUtil()
}
class HttpClientUtil {
  private var httpClient: CloseableHttpClient = _
  private val lock : AnyRef = new Object()

  private val ctx: SSLContext = SSLContext.getInstance("TLS")
  private val tm = new X509TrustManager() {
    override def getAcceptedIssuers :Array[X509Certificate]  =    null
    @throws[CertificateException]
    override def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}
    @throws[CertificateException]
    override def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}
  }
  ctx.init(null, Array[TrustManager](tm), null)
  import org.apache.http.conn.ssl.SSLConnectionSocketFactory
  private  val ssl = new SSLConnectionSocketFactory(ctx)
  private  val socketFactoryRegistry: Registry[ConnectionSocketFactory] = RegistryBuilder.create[ConnectionSocketFactory].register("http", PlainConnectionSocketFactory.INSTANCE).register("https", ssl).build
  private  val ccm: PoolingHttpClientConnectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry)
  // 请求重试处理
  private  val httpRequestRetryHandler = new HttpRequestRetryHandler() {
    override def retryRequest(exception: IOException, executionCount: Int, context: HttpContext): Boolean = {
      if (executionCount >= 2) { // 如果已经重试了5次，就放弃
        return false
      }
      if (exception.isInstanceOf[NoHttpResponseException]) { // 如果服务器丢掉了连接，那么就重试
        return true
      }
      if (exception.isInstanceOf[SSLHandshakeException]) { // 不要重试SSL握手异常
        return false
      }
      if (exception.isInstanceOf[InterruptedIOException]) { // 超时
        return false
      }
      if (exception.isInstanceOf[UnknownHostException]) { // 目标服务器不可达
        return false
      }
      if (exception.isInstanceOf[ConnectTimeoutException]) { // 连接被拒绝
        return false
      }
      if (exception.isInstanceOf[SSLException]) { // SSL握手异常
        return false
      }
      val clientContext = HttpClientContext.adapt(context)
      val request = clientContext.getRequest
      // 如果请求是幂等的，就再次尝试
      if (!request.isInstanceOf[HttpEntityEnclosingRequest]) return true
      false
    }
  }


  def getClient: CloseableHttpClient ={
    lock.synchronized{
      if(httpClient==null) {
        httpClient = HttpClients.custom.setConnectionManager(ccm).setRetryHandler(httpRequestRetryHandler).build
        lock.notifyAll()
      }
    }
    httpClient
  }
  def doPost(url:String,  json:String, charset:String):String =  {
    var response:CloseableHttpResponse = null
    Try {
      val httpPost = new HttpPost(url)
      val stringEntity = new StringEntity(json,charset)//解决中文乱码问题
      stringEntity.setContentType("application/json")
      httpPost.setEntity(stringEntity)
      response = getClient.execute(httpPost)
      val statusCode = response.getStatusLine.getStatusCode
      if (statusCode != HttpStatus.SC_OK) {
        throw new Exception(s"Response failed!,msg = ${response.toString}")
      }
    }match {
        case Success(_)=>
          val entity = response.getEntity
          if (entity == null)
            response.toString
          else
            EntityUtils.toString(response.getEntity, charset)
        case Failure(e)=>
          throw e
      }
  }
}
