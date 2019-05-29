package com.haima.sage.bigdata.etl.driver

import java.util.Properties

import com.haima.sage.bigdata.etl.common.model.{AuthType, KafkaSource, KafkaWriter}
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.common.config.SslConfigs
import org.junit.Test

/**
  * Created by liyju on 2017/9/27.
  */
@Test
class KafkaDriverTest {
  val hostPorts="10.10.106.69:9092,10.10.106.70:9092,10.10.106.89:9092,10.10.106.90:9092"
  val consumerTopic =Some("test_consumer")
  val producerTopic =  Some("test_producer")

  @Test
  def kafkaWriterDriverTest():Unit = {
    val writer =  KafkaWriter(hostPorts=hostPorts,topic = producerTopic,authentication=None)
    kafkaDriverTest(writer)
  }
  @Test
  def kafkaSourceDriverTest():Unit = {
    val source =  KafkaSource(hostPorts=hostPorts,topic = consumerTopic)
    kafkaDriverTest(source)
  }

  def kafkaDriverTest(kafkaMate:KafkaMate):Unit={
    val kafkaDriver = KafkaDriver(kafkaMate)
    kafkaMate.authentication match {
      case Some(AuthType.KERBEROS) =>
        var kerberosPathIsExist = false
        var authConfigIsExist = false
        var principalIsExist = false
        kafkaMate.properties match {
          case Some(prop)=>
            prop.get("kerberos_path") match {
              case Some(a: String) if a != "" =>
                kerberosPathIsExist = true
              case _ =>
                if(System.getProperty("java.security.krb5.conf",null)!= null)
                  kerberosPathIsExist = true
            }
            prop.get("auth_config") match {
              case Some(a: String) if a != "" =>
                authConfigIsExist = true
              case _ =>
                if(System.getProperty("java.security.auth.login.config",null)!=null)
                  authConfigIsExist = true
            }
            prop.get("principal") match {
              case Some(a: String) if a != "" =>
                principalIsExist = true
              case _ =>
                if(System.getProperty("sasl.kerberos.service.name",null)!=null)
                  principalIsExist = true
            }
          case None =>
        }
        if(kerberosPathIsExist&&authConfigIsExist&&principalIsExist)
          assert(kafkaDriver.driver().isSuccess)
        else
          assert(kafkaDriver.driver().isFailure)
      case Some(AuthType.SSL) =>
        var trustFileIsExist = false
        var trustPasswordIsExist = false
        var keystoreFileIsExist = false
        var keystorePasswordIsExist = false
        var keyPasswordIsExist = false
        kafkaMate.properties match {
          case Some(prop)=>
            prop.get("trust_file") match {
              case Some(a: String) if a != "" =>
                trustFileIsExist = true
              case _ =>
                if(System.getProperty(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,null)!=null)
                  trustFileIsExist = true
            }
            prop.get("trust_password") match {
              case Some(a: String) if a != "" =>
                trustPasswordIsExist = true
              case _ =>
                if(System.getProperty(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,null)!=null)
                  trustPasswordIsExist = true
            }
            prop.get("keystore_file") match {
              case Some(a: String) if a != "" =>
                keystoreFileIsExist = true
              case _ =>
                if(System.getProperty(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG,null)!=null)
                  keystoreFileIsExist = true
            }
            prop.get("keystore_password") match {
              case Some(a: String) if a != "" =>
                keystorePasswordIsExist = true
              case _ =>
                if(System.getProperty(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,null)!=null)
                  keystorePasswordIsExist = true
            }
            prop.get("key_password") match {
              case Some(a: String) if a != "" =>
                keyPasswordIsExist = true
              case _ =>
                if(System.getProperty(SslConfigs.SSL_KEY_PASSWORD_CONFIG,null)!=null)
                  keyPasswordIsExist = true
            }
          case None =>
        }
        if(trustFileIsExist&&trustPasswordIsExist&&keystoreFileIsExist&&keyPasswordIsExist&&keystorePasswordIsExist)
          assert(kafkaDriver.driver().isSuccess)
        else
          assert(kafkaDriver.driver().isFailure)
      case Some(AuthType.PLAIN) =>
        var authConfigIsExist = false
        kafkaMate.properties match {
          case Some(prop)=>
            prop.get("auth_config") match {
              case Some(a: String) if a != "" =>
                authConfigIsExist = true
              case _ =>
                if(System.getProperty("java.security.auth.login.config",null)!=null)
                  authConfigIsExist = true
            }
          case None =>
        }
        if(authConfigIsExist)
          assert(kafkaDriver.driver().isSuccess)
        else
          assert(kafkaDriver.driver().isFailure)
      case _ =>
        assert(kafkaDriver.driver().isSuccess)
    }
  }
}