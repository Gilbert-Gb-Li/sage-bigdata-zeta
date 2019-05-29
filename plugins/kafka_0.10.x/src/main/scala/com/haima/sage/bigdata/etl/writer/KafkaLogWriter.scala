package com.haima.sage.bigdata.etl.writer

import java.io.IOException
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.haima.sage.bigdata.etl.common.exception.LogWriteException
import com.haima.sage.bigdata.etl.common.model.Opt._
import com.haima.sage.bigdata.etl.common.model.writer.NameFormatter
import com.haima.sage.bigdata.etl.common.model.{RichMap, _}
import com.haima.sage.bigdata.etl.driver.KafkaDriver
import com.haima.sage.bigdata.etl.metrics.MeterReport
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.KafkaException

import scala.annotation.tailrec
import scala.util.{Failure, Success}


@SuppressWarnings(Array("serial"))
class KafkaLogWriter(conf: KafkaWriter, report: MeterReport) extends DefaultWriter[KafkaWriter](conf, report: MeterReport) with BatchProcess {
  private final val mapper = Formatter(conf.contentType)
  private final val formatter = NameFormatter(conf.topic.getOrElse("topic"), conf.persisRef)
  private final val queue = new AtomicInteger(0)
  private lazy final val count = new AtomicInteger(0)
  private final val start = new AtomicLong(System.currentTimeMillis())
  private val driver = KafkaDriver(conf).driver()

  private var producer = driver match {
    case Success(config) =>
      new KafkaProducer[String, Array[Byte]](config)
    case Failure(e) =>
      throw e
  }


  def reconnect(): Unit = {
    try {
      producer.close()
    } catch {
      case e: Exception =>
        logger.warn(s" kafka write close  $e ")
    }
    producer = driver match {
      case Success(config) =>
        new KafkaProducer[String, Array[Byte]](config)
      case Failure(e) =>
        throw e
    }
  }

  var sendFailFlag = false //是否有发送失败的记录标志

  @tailrec
  final def flush() {
    if (getCached > 0) {
      try {

        //调用此方法可使所有缓冲记录立即可用于发送（即使<code> linger.ms </ code>大于0），并在完成与这些记录相关联的请求时阻塞。
        producer.flush() //kakfa关闭时flush不抛异常，坑！！！
        //确认record是否发送成功
        if (queue.get() <= 0) {
          report()
          if (logger.isDebugEnabled) {
            logger.debug(s"kafka[${Thread.currentThread()}] send data[${count.get()}] take ${System.currentTimeMillis() - start.get()} ms")
            start.set(System.currentTimeMillis())
            count.set(0)
          }
        } else {
          throw new LogWriteException("reFlush")
        }
      } catch {
        case _: LogWriteException =>
          flush()
        case e: Exception =>
          logger.error(s"flush to kafka error: ${e.getMessage} ")
          reconnect()
          flush()
      }
    }
  }


  def newCallback(record: ProducerRecord[String, Array[Byte]]): Callback =
    new Callback {
      override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
        if (e == null) {
          if (!connect) {
            context.parent ! (self.path.name, ProcessModel.WRITER, Status.RUNNING)
            connect = true
          }
          queue.decrementAndGet()
        } else {
          connect = false
          send(record)
          context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, e.getMessage)
        }
      }
    }


  def send(record: ProducerRecord[String, Array[Byte]]): Unit = {
    try {
      producer.send(record, newCallback(record))
      if (logger.isDebugEnabled())
        count.incrementAndGet()
      queue.incrementAndGet()
    } catch {
      case e: InterruptedException =>
        logger.error(s"write to kafka InterruptedException: ${e.getMessage}")
        context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, e.getMessage)
        connect = false
        self ! FLUSH
        close()
      case e: BufferExhaustedException =>
        logger.error(s"write to kafka BufferExhaustedException: ${e.getMessage}")
        context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, e.getMessage)
        connect = false
        self ! FLUSH
        close()
      case e: KafkaException =>
        logger.error(s"write to kafka KafkaException: ${e.getMessage}")
        context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, e.getMessage)
        connect = false
        self ! FLUSH
        close()
      case e: Exception =>
        logger.error(s"write to kafka Exception: ${e.getMessage}")
        context.parent ! (self.path.name, ProcessModel.WRITER, Status.ERROR, e.getMessage)
        connect = false
        self ! FLUSH
        close()
    }
  }

  @throws(classOf[LogWriteException])
  def write(t: RichMap) {


    val record = new ProducerRecord[String, Array[Byte]](formatter.format(t), mapper.bytes(t))
    send(record)
    if (getCached % cacheSize == 0) {
      this.flush()
    }
  }


  @throws(classOf[IOException])
  def close() {
    if (connect || getCached == 0) {
      context.parent ! (self.path.name, ProcessModel.WRITER, Status.STOPPED)
      producer.close()
      context.stop(self)
    } else {
      self ! (self.path.toString, STOP)
    }
  }


  override def redo(): Unit = {

  }
}