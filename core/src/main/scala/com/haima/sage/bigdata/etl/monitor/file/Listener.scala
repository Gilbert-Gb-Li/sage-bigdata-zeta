package com.haima.sage.bigdata.etl.monitor.file

import java.io.IOException
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import akka.actor.{Actor, ActorRef, Props}
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.utils.PathWildMatch

import scala.annotation.tailrec
import scala.collection.immutable.HashMap
import scala.concurrent.duration._
import scala.util.Try

/**
  * Created by zhhuiyan on 15/6/2.
  */
trait Listener[Wrapper <: FileWrapper[Wrapper]] extends FileHandler[Wrapper] with PathWildMatch[Wrapper] {



  private var fileStatus: Map[String, Long] = new HashMap[String, Long]
  protected var running: Boolean = true
  protected var PROCESSOR_THREAD_WAIT_TIMES: Long = Try(Constants.CONF.getLong(PROCESS_WAIT_TIMES)).getOrElse(1)

  @tailrec
  private def recursion(data: List[(Wrapper, Array[Pattern])], first: Boolean = false): Unit = {
    while (running && processed.size() >= pool) {
      TimeUnit.MILLISECONDS.sleep(WAIT_TIME)
    }
    if (running) {
      data match {
        case Nil =>
        case head :: tail =>
          logger.debug(s" monitor :" + head._1.absolutePath)
          val result: List[(Wrapper, Array[Pattern])] = try {
            if (head._2.length == 0) {
              if (!head._1.isDirectory) {
                val changed = if (!first) {
                  fileStatus.get(head._1.absolutePath) match {
                    case None =>
//                      logger.debug(s"find an new file[${head._1.absolutePath}]")
                      process(head._1, true, true)
                    case Some(modifies) =>
                      if (head._1.lastModifiedTime > modifies) {
//                        logger.debug(s"find an changed file[${head._1.absolutePath}]")
                        process(head._1, false, false)
                      } else {
                        false
                      }
                  }
                } else {
                  process(head._1, first, !first)
                }
                if (changed) {
                  fileStatus += ((head._1.absolutePath, head._1.lastModifiedTime))
                }
                List()
              } else {
                val tl = head._1.listFiles().map((_, Array[Pattern]())).toList
                head._1.close()
                tl
              }
            } else {
              if (head._1.isDirectory) {
                val tl = if (head._2.length == 1) {
                  head._1.listFiles().filter(sub => sub.exists() && wildMatch(head._2(0), sub.name)).map((_, Array[Pattern]())).toList
                } else {
                  head._1.listFiles().filter(sub => sub.exists() && wildMatch(head._2(0), sub.name)).map((_, head._2.slice(1, head._2.length))).toList
                }
                head._1.close()
                tl
              } else {
                List()
              }
            }
          } catch {
            case e: Exception =>
              logger.error("monitor ERROR:{}", e)
              List()
          }
          recursion(tail ++ result, first)
      }
    }
  }

  /**
    * monitor an path that
    * when  it is file handle it
    * else if is directory check sub file
    * when sub is directory monitor it
    * else add sub status
    *
    * @param file
    */
  protected def monitor(file: Wrapper, patterns: Array[Pattern] = Array()) {
    recursion(List((file, patterns)), false)
  }


  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def check(file: Wrapper, patterns: Array[Pattern] = Array()) {
    recursion(List((file, patterns)))


  }

  @throws[Exception]
  override def run(): Unit = {
    try {
      val (path, patterns) = parse(source.path)
      val model = if (path.isDirectory) {
        if (patterns.nonEmpty)
          ListenModel.REGEX
        else
          ListenModel.DICTIONARY
      } else {
        ListenModel.FILE
      }
      val listener = context.actorOf(Props.create(classOf[Listener], this).withDispatcher("akka.dispatcher.processor"))
      listener ! (path, patterns, true)
      context.actorOf(Props.create(classOf[Watcher], this, listener, path, patterns).withDispatcher("akka.dispatcher.processor")) ! Opt.WATCH
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logger.error(s" when read create:$e")
        context.parent ! (ProcessModel.MONITOR, Status.ERROR,s"MONITOR_ERROR:${e.getMessage}")
        context.stop(self)
    }
  }

  override def close(): Unit = {
    logger.debug("Listener closed")

    this.running = false
  }

  class Listener extends Actor {



    override def receive: Receive = {
      case (file: Wrapper@unchecked, patterns: Array[Pattern], first: Boolean) =>
        if (running) {
//          logger.debug(s" monitor1 :" + file.absolutePath)
          val result: List[(Wrapper, Array[Pattern], Boolean)] = try {
            if (patterns.length == 0) {
              if (!file.isDirectory) {
                /*
                  代码功能: 判断processed文件队列数目是否大于指定的pool大小。
                  详细描述: 如果队列里面文件数太多，则等待若干时间再次尝试发送消息。
                            如果队列里面文件数很少，则直接发送到队列等待work执行。
                */
                if (processed.size() >= pool) {
                  context.system.scheduler.scheduleOnce(WAIT_TIME millisecond) {
                    self ! (file, patterns, first)
                  }
                } else {
                  val changed = if (!first) {
                    /*
                      代码功能: 判断文件状态信息的。
                      详细描述: 如果文件在fileStatus集合里面没有找到，则该文件是新文件，直接发送到给processed文件队列。
                                如果文件在fileStatus集合里面匹配到(即已经处理过)，则判断文件最新修改时间是否大于上次记录的时间。
                                如果大于则直接发送到给processed文件队列。如果相等则不做任何处理。
                    */
                    fileStatus.get(file.absolutePath) match {
                      case None =>
//                        logger.debug(s"find an new file[${file.absolutePath}]")
                        process(file, true, true)
                      case Some(modifies) =>
                        if (file.lastModifiedTime > modifies) {
//                          logger.debug(s"find an changed file[${file.absolutePath}]")
                          process(file, false, false)
                        } else {
                          false
                        }
                    }
                  } else {
                    process(file, first, !first)
                  }
                  if (changed) {
                    fileStatus += ((file.absolutePath, file.lastModifiedTime))
                  }
                }
                List()
              } else {
                var t1: List[(Wrapper, Array[Pattern], Boolean)] = List()
                val t2 = file.listFiles()
                while(t2.hasNext) {
                  val wrapper = t2.next()
                  t1 = (wrapper, Array[Pattern](), first) +: t1
                }
                file.close()
                t1
              }
            } else {
              if (file.isDirectory) {
                val tl = file.listFiles()
                val t1 = tl.filter(sub => {
                  if (sub.exists() && wildMatch(patterns(0), sub.name)) {
                    true
                  } else {
                    sub.close()
                    false
                  }
                }).map((_, Array[Pattern](), first)).toList
                file.close()
                t1
              } else {
                List()
              }
            }
          } catch {
            case e: Exception =>
              context.parent ! (Opt.WAITING,Status.UNKNOWN,"Exception:File not found in HDFS")
              logger.error("monitor ERROR:{}", e)
              List()
          }
          if (result.nonEmpty) {
            result.foreach{
              x => self ! x
            }
          }
        } else {
          context.stop(self)
        }
    }
  }

  class Watcher(listener: ActorRef, file: Wrapper, patterns: Array[Pattern]) extends Actor {
    override def receive: Receive = {
      case Opt.WATCH =>
        if (!running) {
          context.stop(self)
        } else {
          if (!(processed.size() >= pool)) {
            listener ! (getFileWrapper(file.absolutePath), patterns, false)
          }
          context.system.scheduler.scheduleOnce(WAIT_TIME*10 millisecond) {
            if (running)
              self ! Opt.WATCH
          }
        }


    }
  }

}
