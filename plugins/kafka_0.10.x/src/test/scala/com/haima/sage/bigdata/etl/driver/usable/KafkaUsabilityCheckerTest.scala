package com.haima.sage.bigdata.etl.driver.usable
import com.haima.sage.bigdata.etl.common.model.{KafkaSource, KafkaWriter}
import org.junit.Test
import com.haima.sage.bigdata.etl.utils.Logger

/**
  * Created by liyju on 2017/9/27.
  */
@Test
class KafkaUsabilityCheckerTest  extends Logger{
  /**
    * 数据存储校验地址正确，topic存在
    */
  @Test
  def kafkaWriterCheckerTestA():Unit={
    val writer =  KafkaWriter(hostPorts="10.10.106.86:9092,10.10.106.85:9092,10.10.106.84:9092",topic = Some("test_sage-bigdata-etl"),authentication=None)
    val kafkaUsabilityChecker =KafkaUsabilityChecker(writer)
    assert(kafkaUsabilityChecker.check.usable)
  }

  /**
    * 数据存储校验地址正确，topic不存在
    */
  @Test
  def kafkaWriterCheckerTestB():Unit={
    val writer =  KafkaWriter(hostPorts="10.10.106.86:9092,10.10.106.85:9092,10.10.106.84:9092",topic = Some("test_sage-bigdata-etlcfdre"),authentication=None)
    val kafkaUsabilityChecker =KafkaUsabilityChecker(writer)
    assert(!kafkaUsabilityChecker.check.usable)
  }

  /**
    * 数据存储校验部分地址正确，topic不存在
    */
  @Test
  def kafkaWriterCheckerTestC():Unit={
    val writer =  KafkaWriter(hostPorts="10.10.106.86:9092,10.10.106.85:9092,10.10.106.84:9096",topic = Some("test_sage-bigdata-etlcfdre"),authentication=None)
    val kafkaUsabilityChecker =KafkaUsabilityChecker(writer)
    assert(!kafkaUsabilityChecker.check.usable)
  }


  /**
    * 数据存储校验部分地址正确，topic存在
    */
  @Test
  def kafkaWriterCheckerTestD():Unit={
    val writer =  KafkaWriter(hostPorts="10.10.106.86:9092,10.10.106.85:9096,10.10.106.84:9096",topic = Some("test_sage-bigdata-etl"),authentication=None)
    val kafkaUsabilityChecker =KafkaUsabilityChecker(writer)
    assert(kafkaUsabilityChecker.check.usable)
  }

  /**
    * 数据存储校验集群地址不可用
    */
  @Test
  def kafkaWriterCheckerTestE():Unit={
    val writer =  KafkaWriter(hostPorts="10.10.106.86:9096,10.10.106.85:9096,10.10.106.84:9096",topic = Some("test_sage-bigdata-etl"),authentication=None)
    val kafkaUsabilityChecker =KafkaUsabilityChecker(writer)
    assert(!kafkaUsabilityChecker.check.usable)
  }

  /**
    * 数据存储校验集群地址格式不正确
    */
  @Test
  def kafkaWriterCheckerTestF():Unit={
    val writer =  KafkaWriter(hostPorts="10.10.106.86;9092,10.10.106.85;9092,10.10.106.84;9092",topic = Some("test_sage-bigdata-etl"),authentication=None)
    val kafkaUsabilityChecker =KafkaUsabilityChecker(writer)
    assert(!kafkaUsabilityChecker.check.usable)
  }

  /**
    * 数据源校验集群地址正确，topic配置有错误的情况
    */
  @Test
  def kafkaSourceCheckerTestA():Unit={
    val source =  KafkaSource(hostPorts="10.10.106.86:9092,10.10.106.85:9092,10.10.106.84:9092",topic = Some("test_sage-bigdata-etl;test_sage-bigdata-etl:"),authentication=None)
    val kafkaUsabilityChecker =KafkaUsabilityChecker(source)
    assert(!kafkaUsabilityChecker.check.usable)
  }

  /**
    * 数据源校验集群地址正确，topic正确
    */
  @Test
  def kafkaSourceCheckerTestB():Unit={
    val source =  KafkaSource(hostPorts="10.10.106.86:9092,10.10.106.85:9092,10.10.106.84:9092",topic = Some("test_sage-bigdata-etl"),authentication=None)
    val kafkaUsabilityChecker =KafkaUsabilityChecker(source)
    assert(kafkaUsabilityChecker.check.usable)
  }

  /**
    * 数据源校验集群地址不正确，topic配置有错误的情况
    */
  @Test
  def kafkaSourceCheckerTestC():Unit={
    val source =  KafkaSource(hostPorts="10.10.106.86:9096,10.10.106.85:9096,10.10.106.84:9096",topic = Some("test_sage-bigdata-etl;test_sage-bigdata-etl:"),authentication=None)
    val kafkaUsabilityChecker =KafkaUsabilityChecker(source)
    assert(!kafkaUsabilityChecker.check.usable)
  }

  /**
    * 数据源校验集群地址不正确，topic正确
    */
  @Test
  def kafkaSourceCheckerTestD():Unit={
    val source =  KafkaSource(hostPorts="10.10.106.86:9096,10.10.106.85:9096,10.10.106.84:9096",topic = Some("test_sage-bigdata-etl"),authentication=None)
    val kafkaUsabilityChecker =KafkaUsabilityChecker(source)
    assert(!kafkaUsabilityChecker.check.usable)
  }


  /**
    * 数据源校验集群地址部分正确，topic配置有错误的情况
    */
  @Test
  def kafkaSourceCheckerTestE():Unit={
    val source =  KafkaSource(hostPorts="10.10.106.86:9092,10.10.106.85:9092,10.10.106.84:9096",topic = Some("test_sage-bigdata-etl;test_sage-bigdata-etl:"),authentication=None)
    val kafkaUsabilityChecker =KafkaUsabilityChecker(source)
    assert(!kafkaUsabilityChecker.check.usable)
  }

  /**
    * 数据源校验集群地址部分正确，topic正确
    */
  @Test
  def kafkaSourceCheckerTestF():Unit={
    val source =  KafkaSource(hostPorts="10.10.106.86:9092,10.10.106.85:9092,10.10.106.84:9096",topic = Some("test_sage-bigdata-etl"),authentication=None)
    val kafkaUsabilityChecker =KafkaUsabilityChecker(source)
    assert(kafkaUsabilityChecker.check.usable)
  }

  /**
    * 数据源校验集群地址格式全不正确，topic配置有错误的情况
    */
  @Test
  def kafkaSourceCheckerTestG():Unit={
    val source =  KafkaSource(hostPorts="10.10.106.86;9092,10.10.106.85;9092,10.10.106.84;9092",topic = Some("test_sage-bigdata-etl;test_sage-bigdata-etl:"),authentication=None)
    val kafkaUsabilityChecker =KafkaUsabilityChecker(source)
    assert(!kafkaUsabilityChecker.check.usable)
  }

  /**
     * 数据源校验集群地址格式全不正确，topic正确
     */
   @Test
   def kafkaSourceCheckerTestH():Unit={
     val source =  KafkaSource(hostPorts="10.10.106.86;9092,10.10.106.85;9092,10.10.106.84;9092",topic = Some("test_sage-bigdata-etl"),authentication=None)
     val kafkaUsabilityChecker =KafkaUsabilityChecker(source)
     assert(!kafkaUsabilityChecker.check.usable)
   }

  /**
    * 数据源校验集群地址格式正确，topic正确,部分topic不存在
    */
  @Test
  def kafkaSourceCheckerTestI():Unit={
    val source =  KafkaSource(hostPorts="10.10.106.86:9092,10.10.106.85:9092,10.10.106.84:9092",topic = Some("test_sage-bigdata-etl;aaaa"),authentication=None)
    val kafkaUsabilityChecker =KafkaUsabilityChecker(source)
    assert(!kafkaUsabilityChecker.check.usable)
  }

  /**
    * 数据源校验集群地址格式正确，正则匹配topic，有满足正则的topic
    */
  @Test
  def kafkaSourceCheckerTestL():Unit={
    val source =  KafkaSource(hostPorts="10.10.106.86:9092,10.10.106.85:9092,10.10.106.84:9092",topic = Some("test*"),authentication=None,wildcard = "true")
    val kafkaUsabilityChecker =KafkaUsabilityChecker(source)
    assert(kafkaUsabilityChecker.check.usable)
  }

  /**
    * 数据源校验集群地址格式正确，正则匹配topic，没有有满足正则的topic
    */
  @Test
  def kafkaSourceCheckerTestM():Unit={
    val source =  KafkaSource(hostPorts="10.10.106.86:9092,10.10.106.85:9092,10.10.106.84:9092",topic = Some("acbd*"),authentication=None,wildcard = "true")
    val kafkaUsabilityChecker =KafkaUsabilityChecker(source)
    assert(!kafkaUsabilityChecker.check.usable)
  }
}
