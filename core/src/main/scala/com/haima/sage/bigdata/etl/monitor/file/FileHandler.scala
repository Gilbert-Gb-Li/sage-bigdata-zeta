package com.haima.sage.bigdata.etl.monitor.file

import java.io.FileNotFoundException
import java.time.temporal.ChronoUnit
import java.util.Comparator
import java.util.concurrent.PriorityBlockingQueue

import akka.util.Timeout
import com.haima.sage.bigdata.etl.codec.Codec
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants._
import com.haima.sage.bigdata.etl.common.exception.LogReaderInitException
import com.haima.sage.bigdata.etl.common.model.ProcessFrom._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.monitor.Monitor
import com.haima.sage.bigdata.etl.reader.FileLogReader
import com.haima.sage.bigdata.etl.utils.FileUtils

import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Created by zhhuiyan on 2014/11/15.
  */


trait FileHandler[Wrapper <: FileWrapper[Wrapper]] extends Monitor {


  object ListenModel extends Enumeration {
    type Model = Value
    val FILE, DICTIONARY, REGEX = Value
  }


  implicit var id: String = _
  protected val utils: FileUtils

  protected def source: FileSource

  protected def parser: Parser[MapRule]

  protected var AGENT_FILE_READER_THREAD_BUFFER: Integer = CONF.getInt(READER_FILE_BUFFER) match {
    case 0 =>
      READER_FILE_BUFFER_DEFAULT
    case a: Int =>
      a
  }

  def getFileWrapper(path: String): Wrapper

  /*
  * 文件缓存
  * */
  private final lazy val files = new PriorityBlockingQueue[(String, Boolean, Boolean, Long)]((size + 1).toInt, new Comparator[(String, Boolean, Boolean, Long)] {
    override def compare(o1: (String, Boolean, Boolean, Long), o2: (String, Boolean, Boolean, Long)): Int = {
      (o1._4 - o2._4).toInt
    }
  })
  /*
  *
  * 缓存的文件个数大小
  * */
  private lazy val size = Constants.CONF.getLong("worker.process.file.max-cache")
  /*
  *
  * 只处理新发现的文件
  * */
  private lazy val onlyNew = Constants.CONF.getBoolean("worker.process.file.only-new")

  /*
  *延时处理 if (delay==0) no delay
  *
  * */
  private lazy val delay = Constants.CONF.getDuration("worker.process.file.delay-after-create").get(ChronoUnit.NANOS) / 1000000

  def add(file: Wrapper, first: Boolean, isCreate: Boolean): Unit = {
    import scala.collection.JavaConversions._
    if (!files.exists(_._1 == file.absolutePath)) {

      files.add((file.absolutePath, first, isCreate, System.currentTimeMillis()))
      file.close()
      /*
      * 只有有延时处理,切是创建的时候,加等待时间后处理
      * */
      if (delay == 0)
        context.system.scheduler.scheduleOnce(delay seconds) {
          self ! Opt.GET
        }
    }


  }

  def execute(wrapper: Wrapper, first: Boolean, isCreate: Boolean)(implicit id: String): Boolean = {
    if (!processed.contains(wrapper.uri)) {
      processed.add(wrapper.uri)
      val tuple = handle(wrapper, first, isCreate)
      tuple match {
        case Some(reader) =>
          send(Some(reader), Some(parser))
          true
        case _ =>
          wrapper.close()
          self ! (Opt.DELETE, wrapper.uri)
          processed.remove(wrapper.uri)
          false
      }
    } else {
      false
    }
  }


  def process(wrapper: Wrapper, first: Boolean, isCreate: Boolean)(implicit id: String): Boolean = {


    /*
    *
    * 只处理最新文件逻辑,忽略老文件
    * */
    if (onlyNew && first) {
      false
    } else {
      /*
      * else 处理线程满,或者延时处理
      * */
      if (processed.size() < pool && delay == 0) {
        execute(wrapper, first, isCreate)
      } else {
        add(wrapper: Wrapper, first: Boolean, isCreate: Boolean)
        if (files.size() > size) {
          files.poll()
          logger.warn(s"file cache waiting for process is max then $size remove old")
        }
        true
      }
    }


  }


  override def receive: Receive = {
    case id: String =>
      this.id = id

    case Opt.GET =>
      if (processed.size() < pool) {
        if (!files.isEmpty) {
          val data = files.peek()
          /*
          * 文件已经被处理,或者被删除,取下一个文件
          */
          if (data != null) {
            val file = getFileWrapper(data._1)
            if (file.exists()) {
              /*
            * 没有延时,移出队列,处理文件
            * */
              if (delay == 0) {
                files.poll()
                execute(file, data._2, data._3)
              } else {
                /*
                * 延时的时间到了,移出队列,处理文件,
                * */
                if (data._4 + delay <= System.currentTimeMillis()) {
                  files.poll()
                  execute(file, data._2, data._3)
                }
              }
            }


          } else {
            self ! Opt.GET
          }

        }


      }
    case (Opt.DELETE, path: String) =>

      logger.info(s"monitor remove path[$path]")
      processed.remove(path)
      self ! Opt.GET

    case (Opt.WAITING,Status.UNKNOWN,msg:String) =>
      logger.info(s"Exception:$msg")
      context.parent ! (ProcessModel.MONITOR,Status.UNKNOWN,msg)

    case obj =>
      super.receive(obj)
  }

  /**
    *
    * @param wrapper 文件wrapper
    * @param first 第一次启动
    * @param create 文件是新建的
    * @param id 数据通道
    * @return fileReader
    */
  @SuppressWarnings(Array("unchecked"))
  private def handle(wrapper: Wrapper, first: Boolean, create: Boolean)(implicit id: String): Option[FileLogReader[_]] = {
    try {
      val modified = wrapper.lastModifiedTime
      var stressFlag = false  ///监听到文件变化，变化是否重读标志
      val uri = wrapper.uri
      import akka.pattern.ask

      import scala.concurrent.duration._
      implicit val timeout = Timeout(5 minutes)
      val position = Await.result(positionServer ? (Opt.GET, s"$id:" + uri), timeout.duration).asInstanceOf[ReadPosition] match {
        case pos if modified == pos.modified && pos.finished && END == source.position.getOrElse(END) =>
          throw new LogReaderInitException(s"$uri has process finished,don't need to read again. records:${pos.records}")
        case pos =>
          if(modified != pos.modified && START == source.position.getOrElse(END)) //监听到文件变化，并且设置了变化重读
            stressFlag=true
          pos.copy(modified = modified)
      }
      //如果文件没有被读取过或是设置了文件变化时重新取得文件
      //或者文件已经被读过，但是修改了数据源的配置,设置了文件变化重读，重启数据通道,则需要对position信息重构
      val logReader =if (position.records <= 0 || stressFlag
        ||(START == source.position.getOrElse(END)&&first)){
        if (source.skipLine > 0) {
          val rd = makeReader(uri: String, wrapper: Wrapper, position: ReadPosition)
          rd.skipLine(source.skipLine)
          rd
        } else {
          position.setPosition(0)
          position.setRecords(0)
          makeReader(uri: String, wrapper: Wrapper, position: ReadPosition)
        }
      } else {
        val rd = makeReader(uri: String, wrapper: Wrapper, position: ReadPosition)
        val skipped: Long = rd.skip(position.position)
        logger.debug(s"$uri need skip:$position skipped:$skipped")
        if (skipped < position.position) {
          rd.close()
          positionServer ! (Opt.FLUSH, position.copy(position = skipped))
          throw new LogReaderInitException(s"$uri has process finished,don't need to  read again. records:${position.records}")
        }
        rd
      }

      logger.debug(s"starting  processor file:$uri")
      Some(logReader)
    } catch {
      case e: LogReaderInitException =>
        logger.info(s"${e.getMessage}")
        None
      case e: Exception =>
        logger.error("FILE HANDLER ERROR: {}", e)
        None

      case ex: Throwable =>
        logger.error(s"FILE HANDLER ERROR: $ex")
        None

    }

  }

  def makeReader(uri: String, wrapper: Wrapper, position: ReadPosition): FileLogReader[_] = {

    val clazz: Class[FileLogReader[_]] = Class.forName(Constants.READERS.getString(
      source.contentType.get.name
    )).asInstanceOf[Class[FileLogReader[_]]]
    clazz.getConstructor(
      classOf[String],
      classOf[Option[Codec]],
      classOf[Option[FileType]],
      classOf[FileWrapper[_]],
      classOf[ReadPosition]).
      newInstance(uri, source.codec, source.contentType, wrapper, position)

  }

}
