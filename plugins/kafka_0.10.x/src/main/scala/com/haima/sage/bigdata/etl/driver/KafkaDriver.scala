package com.haima.sage.bigdata.etl.driver

import java.util.{Properties, UUID}

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.{AuthType, KafkaSource, WithProperties}
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer}

import scala.util.Try

/**
  * Created by zhhuiyan on 2017/2/9.
  */
case class KafkaDriver(mate: KafkaMate) extends WithProperties with Driver[Properties] with Logger {


  def driver() = Try {

    val props = properties match {
      case None =>
        new Properties
      case Some(p) =>
        p
    }
    val WAIT_TIME:Long = Try(Constants.CONF.getLong(Constants.PROCESS_WAIT_TIMES)).getOrElse(Constants.PROCESS_WAIT_TIMES_DEFAULT)
    props.put(Constants.ZOOKEEPER_CONNECTION_TIMEOUT, get(Constants.ZOOKEEPER_CONNECTION_TIMEOUT, "5000"))
    props.put(Constants.ZOOKEEPER_SESSION_TIMEOUT_MS, get(Constants.ZOOKEEPER_SESSION_TIMEOUT_MS, "30000"))
    props.put(Constants.AUTO_OFFSET_RESET, get(Constants.AUTO_OFFSET_RESET, "earliest"))
    props.put("offsets.topic.segment.bytes", "104857600")
    props.put("consumer.timeout.ms", WAIT_TIME + "")
    props.put("num.consumer.fetchers", 5 + "")
    props.put("fetch.min.bytes", 1 + "")
    props.put("fetch.wait.max.ms", WAIT_TIME-10 + "")
   mate match {
     case source: KafkaSource =>
       if("".equals(source.groupId)|| source.groupId==null)
         props.put("group.id", get("group_id",UUID.randomUUID().toString))
       else
         props.put("group.id", get("group_id", source.groupId))
      case _=>props.put("group.id", get("group_id",UUID.randomUUID().toString))
    }
    props.put("client.id", get("client_id", UUID.randomUUID().toString))
    //Consumer session 过期时间。这个值必须设置在broker configuration中的group.min.session.timeout.ms 与 group.max.session.timeout.ms之间。
    //其默认值是：10000 （10 s）
    props.put("session.timeout.ms", "10000")
   //数据提交确认方式 all,-1 :all node ok
    // 0 :none
    // 1:leader ok
    props.put("acks", "1")
    props.put("retries", 0.toString)
    props.put("enable.auto.commit", "false")
    //最大缓存条数
    props.put("batch.size","20000")
    //启用压缩,gzip 压缩率最高,性能最低,snappy,中等,lz4压缩率最低
    props.put("compression.type","snappy")
    //最大等待时间
    props.put("linger.ms", 50.toString)
    //最大缓存
    props.put("buffer.memory", 33554432.toString)
    //最大请求字节数
    props.put("max.request.size", 33554432.toString)
    props.put("key.serializer", classOf[StringSerializer])
    props.put("value.serializer", classOf[ByteArraySerializer])
    props.put("key.deserializer", classOf[StringDeserializer])
    props.put("value.deserializer", classOf[ByteArrayDeserializer])
    props.put("bootstrap.servers", mate.hostPorts)
    //请求发起后，并不一定会很快接收到响应信息。这个配置就是来配置请求超时时间的。默认值是：305000 （305 s）
    //request.timeout.ms should be greater than session.timeout.ms and fetch.max.wait.ms
    props.put("request.timeout.ms","11000")
    //心跳间隔。心跳是在consumer与coordinator之间进行的,
    //这个值必须设置的小于session.timeout.ms 常设置的值要低于session.timeout.ms的1/3. 默认值是：3000 （3s）
    props.put("heartbeat.interval.ms","3000")
    //当consumer向一个broker发起fetch请求时，broker返回的records的大小最小值。如果broker中数据量不够的话会wait，直到数据大小满足这个条件。
    //取值范围是：[0, Integer.Max]，默认值是1。
    props.put("fetch.min.bytes","0")
    //Fetch请求发给broker后，在broker中可能会被阻塞的（当topic中records的总size小于fetch.min.bytes时），
    //此时这个fetch请求耗时就会比较长。这个配置就是来配置consumer最多等待response多久。
    props.put("fetch.max.wait.ms","1000")
    props.put("max.block.ms","30000")
    mate.authentication match {
      case Some(AuthType.KERBEROS) =>
        logger.debug("KERBEROS config")
        props.setProperty("security.protocol", "SASL_PLAINTEXT")
        props.setProperty("sasl.mechanism", "GSSAPI")
        get("kerberos_path") match {
          case Some(a: String) =>
            System.setProperty("java.security.krb5.conf", a)
          case _ =>
            if(System.getProperty("java.security.krb5.conf",null)==null)
              throw new Exception(s"ssl.truststore.location not found ")

        }
        get("auth_config") match {
          case Some(a: String) =>
            System.setProperty("java.security.auth.login.config", a)
          case _ =>
            if(System.getProperty("java.security.auth.login.config",null)==null)
              throw new Exception(s"ssl.truststore.location not found ")

        }
        get("principal") match {
          case Some(a: String) =>
            props.setProperty("sasl.kerberos.service.name", a)
          case _ =>
            if(System.getProperty("sasl.kerberos.service.name",null)==null)
              throw new Exception(s"sasl.kerberos.service.name not set ")
        }
      case Some(AuthType.SSL) =>
        props.setProperty("security.protocol", "SSL")
        props.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS")
        get("trust_file") match {
          case Some(a: String) =>
            props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, a)
          case _ =>
            if(System.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,null)==null)
              throw new Exception(s"${SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG} not found ")
        }
        get("trust_password") match {
          case Some(a: String) =>
            props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, a)
          case _ =>
            if(System.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,null)==null)
              throw new Exception(s" ${SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG}   not set ")
        }
        get("keystore_file") match {
          case Some(a: String) =>
            props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, a)
          case _ =>
            if(System.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,null)==null)
              throw new Exception(s"${SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG} not found ")
        }
        get("keystore_password") match {
          case Some(a: String) =>
            props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, a)
          case _ =>
            if(System.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,null)==null)
              throw new Exception(s" ${SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG}   not set ")
        }
        get("key_password") match {
          case Some(a: String) =>
            props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, a)
          case _ =>
            if(System.getProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG,null)==null)
              throw new Exception(s" ${SslConfigs.SSL_KEY_PASSWORD_CONFIG}   not set ")
        }
      case Some(AuthType.PLAIN) =>
        logger.debug("PLAIN config")
        props.setProperty("security.protocol", "SASL_PLAINTEXT")
        props.setProperty("sasl.mechanism", "PLAIN")
        get("auth_config") match {
          case Some(a: String) =>
            System.setProperty("java.security.auth.login.config", a)
          case _ =>
            if(System.getProperty("java.security.auth.login.config",null)==null)
              throw new Exception(s"java.security.auth.login.config  file not found")
        }
      case Some(AuthType.PLAIN_SSL) =>
        logger.debug("PLAIN_SSL config:"+mate)
        props.setProperty("security.protocol", "SASL_SSL")
        props.setProperty("sasl.mechanism", "PLAIN")
        get("auth_config") match {
          case Some(a: String) =>
            System.setProperty("java.security.auth.login.config", a)
          case _ =>
            if(System.getProperty("java.security.auth.login.config",null)==null)
              throw new Exception(s"java.security.auth.login.config  file not found")
        }
        props.setProperty(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "JKS")
        get("trust_file") match {
          case Some(a: String) =>
            props.setProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, a)
          case _ =>
            if(System.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,null)==null)
              throw new Exception(s"${SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG} not found ")
        }
        get("trust_password") match {
          case Some(a: String) =>
            props.setProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, a)
          case _ =>
            if(System.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,null)==null)
              throw new Exception(s" ${SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG}   not set ")
        }
        get("keystore_file") match {
          case Some(a: String) =>
            props.setProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, a)
          case _ =>
            if(System.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,null)==null)
              throw new Exception(s"${SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG} not found ")
        }
        get("keystore_password") match {
          case Some(a: String) =>
            props.setProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, a)
          case _ =>
            if(System.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,null)==null)
              throw new Exception(s" ${SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG}   not set ")
        }
        get("key_password") match {
          case Some(a: String) =>
            props.setProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG, a)
          case _ =>
            if(System.getProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG,null)==null)
              throw new Exception(s" ${SslConfigs.SSL_KEY_PASSWORD_CONFIG}   not set ")
        }
      case _ =>
    }
    props
  }

  override def properties: Option[Properties] = mate.properties
}
