package com.haima.sage.bigdata.etl.monitor.file

import java.io._
import java.nio.file.StandardWatchEventKinds._
import java.nio.file._
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.regex.Pattern

import akka.actor.{Actor, ActorRef, Props, Terminated}
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.haima.sage.bigdata.etl.utils.{FileUtils, PathWildMatch}
import com.sun.nio.file.SensitivityWatchEventModifier

import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps


class DirectoryMonitor(override val source: FileSource, override val parser: Parser[MapRule]) extends FileHandler[LocalFileWrapper] with PathWildMatch[LocalFileWrapper] {

  import context.dispatcher

  private lazy val watcher: WatchService = FileSystems.getDefault.newWatchService()
  private lazy val watchKeys: mutable.Map[WatchKey, PathPatterns] = new mutable.HashMap[WatchKey, PathPatterns]
  private lazy val pathWatchKeyMap: mutable.Map[String, WatchKey] = new mutable.HashMap[String, WatchKey]
  /*
  *
  * HIGH,
      MEDIUM,
      LOW;
  * */
  private lazy val WATCH_MODEL = SensitivityWatchEventModifier.valueOf(Constants.CONF.getString("worker.process.file.watch-model"))


  private lazy val listener: ActorRef = context.actorOf(Props.create(classOf[Listener], this))


  private lazy val done: AtomicBoolean = new AtomicBoolean(false)


  @throws(classOf[IOException])
  override def close() {
    this.done.set(true)
    try {
      watcher.close()

    } catch {
      case e:Exception=>
    }
    context.stop(listener)
  }


  case class PathPatterns(path: String,
                          patterns: Array[Pattern] = Array())

  @throws[Exception]
  def run(): Unit = {


    logger.debug("waiting time is {} ", WAIT_TIME)
    val (file, patterns) = parse(source.path)

    try {
      val model = if (file.isDirectory) {
        if (patterns.nonEmpty)
          ListenModel.REGEX
        else {
          logger.info("Director Monitor is running")
          ListenModel.DICTIONARY
        }
      } else {
        ListenModel.FILE
      }
      context.parent ! (ProcessModel.MONITOR, Status.RUNNING)
      // inProcess.incrementAndGet()
      listener ! (file, patterns, true, false)
      context.actorOf(Props.create(classOf[Watcher], this, listener, model).withDispatcher("akka.dispatcher.processor")) ! Opt.WATCH
    } catch {
      case e: Exception =>
        logger.error("create reader error", e)
    }

  }

  override def getFileWrapper(path: String): LocalFileWrapper = {
    val path1 = try {
      Paths.get(path)
    } catch {
      case e: Exception =>
        logger.error(s"check path error :$e")
        null
    }
    LocalFileWrapper(path1, source.encoding)
  }

  override protected val utils: FileUtils = JavaFileUtils

  var i = 0


  class Listener extends Actor {

    import java.nio.file.WatchEvent


    /** 注册文件到监听的缓存
      * */
    @throws[IOException]
    private def register(file: LocalFileWrapper, patterns: Array[Pattern]) {


      val (self: LocalFileWrapper, key: WatchKey, monitor: PathPatterns) =


        if (file.isDirectory) {

          pathWatchKeyMap.get(file.absolutePath) match {
            case Some(k) =>
              (file, k, watchKeys.get(k).orNull)
            case None =>
              /*正则,或者目录,第一次执行*/
              logger.debug(s"register {}", file.absolutePath)
              (file, file.path.register(watcher, Array[WatchEvent.Kind[_]](ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY), WATCH_MODEL), PathPatterns(file.absolutePath, patterns = patterns))
          }
        } else {
          val parent = file.getParent
          pathWatchKeyMap.get(parent.absolutePath) match {
            //需要正则匹配时,监听的是通配符
            case Some(k) =>
              (parent, k, watchKeys.get(k).orNull)
            case None =>
              /*只有监控的是文件,并且是第一次才会执行*/

              (parent, parent.path.register(watcher, Array[WatchEvent.Kind[_]](ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY), WATCH_MODEL), PathPatterns(file.absolutePath, patterns = patterns))

          }
        }
      watchKeys.put(key, monitor)
      pathWatchKeyMap.put(self.absolutePath, key)

    }

    override def receive: Receive = {

      /*已经匹配到的文件*/
      case (file: LocalFileWrapper, patterns: Array[Pattern], starting: Boolean, create: Boolean) =>
        if (!done.get()) {
          /*注册监听*/
          register(file, patterns)
          if (file.isDirectory) {
            /*处理目录*/
            try {
              context.actorOf(Props.create(classOf[DirectoryActor], this, (file, patterns, starting, create))) ! Opt.GET
            } catch {
              case e: Exception =>
                e.printStackTrace()
                throw e
            }

          } else {
            try {
              /*处理文件*/

              if (file.exists() && ((patterns.length == 0) || (patterns.length == 1 && wildMatch(patterns(0), file.name)))) {
                process(file, starting, create)


                //
              }
            } catch {
              case e: Exception =>
                e.printStackTrace()
                logger.error(s"monitor must ignore file handle error: {}", e.getMessage)
            }

          }
        } else {
          context.stop(self)
        }
      case Status.ERROR =>
        logger.info("File is not exist now")
        context.parent ! (Opt.WAITING, Status.UNKNOWN, "Exception:File is not exist now")
        //因为要轮询，所以不能自杀
        //context.stop(self)
    }

    class DirectoryActor(tuple: (LocalFileWrapper, Array[Pattern], Boolean, Boolean)) extends Actor {

      lazy val stream: DirectoryStream[Path] = Files.newDirectoryStream(tuple._1.path)
      lazy val directory: util.Iterator[Path] = stream.iterator()
      var isMatch: Boolean = false

      override def receive: Receive = {
        case Opt.GET =>
          if (!done.get()) {

            if (directory.hasNext) {
              val file = LocalFileWrapper(directory.next(), tuple._1._encoding)
              /*当文件存在时*/
              if (!(file.name.endsWith(".DS_Store") || file.name.endsWith(".swp")) && file.exists()) {
                if (tuple._2.length == 0) {
                  isMatch = true
                  inQueue((file, tuple._2, tuple._3, tuple._4))
                } else if ((file.isDirectory && wildMatch(tuple._2(0), file.name)) || (!file.isDirectory && tuple._2.length == 1 && wildMatch(tuple._2(0), file.name))) {
                  /*当是目录切正则匹配当前路径时,或者是文件切完整匹配正则时*/
                  isMatch = true
                  inQueue((file, tuple._2.slice(1, tuple._2.length), tuple._3, tuple._4))
                }

              }

              context.system.scheduler.scheduleOnce(1 milliseconds) {
                self ! Opt.GET
              }
            } else {
              if (!isMatch) {
                logger.info("File not exist.")
                listener ! Status.ERROR
              }
              //stream.close()
              //context.stop(self)
            }
          } else {
            stream.close()
            context.stop(self)
          }


      }
    }


  }

  def inQueue(data: (LocalFileWrapper, Array[Pattern], Boolean, Boolean)): Unit = {
    listener ! data

    /*if (processed.size() < pool) {
      queueServer ! Opt.GET
    }*/
  }

  class Watcher(listener: ActorRef, model: ListenModel.Model) extends Actor {


    class EventProcessor() extends Actor {


      private def process(event: WatchEvent[_], monitor: PathPatterns): Unit = {
        val name = cast(event).context
        event.kind match {
          /*文件删除*/
          case ENTRY_DELETE if model == ListenModel.FILE =>
            val child = LocalFileWrapper(Paths.get(monitor.path).getParent.resolve(name), source.encoding)
            val child_key = pathWatchKeyMap.remove(child.absolutePath)
            if (child_key.isDefined) {
              logger.debug(s"delete  path:${child.absolutePath}")
              watchKeys.remove(child_key.get)
            }

          /*文件夹删除*/
          case ENTRY_DELETE =>
            val child = LocalFileWrapper(Paths.get(monitor.path).resolve(name), source.encoding)
            val child_key = pathWatchKeyMap.remove(child.absolutePath)
            if (child_key.isDefined) {
              logger.debug(s"delete  path:${child.absolutePath}")
              watchKeys.remove(child_key.get)
            }

          /*文件删除,然后创建*/
          case ENTRY_CREATE if model == ListenModel.FILE =>
            val child = LocalFileWrapper(Paths.get(monitor.path).getParent.resolve(name), source.encoding)
            if (getFileWrapper(monitor.path) == child && !child.isDirectory) {
              inQueue((child, Array[Pattern](), false, true))
            }
          /*新增目录,或者文件*/
          case ENTRY_CREATE if model == ListenModel.DICTIONARY =>
            val child = LocalFileWrapper(Paths.get(monitor.path).resolve(name), source.encoding)
            inQueue((child, Array[Pattern](), false, true))

          /*新增正则*/
          case ENTRY_CREATE if model == ListenModel.REGEX && monitor.patterns.nonEmpty =>
            val child = LocalFileWrapper(Paths.get(monitor.path).resolve(name), source.encoding)
            if (wildMatch(monitor.patterns(0), child.name)) {
              logger.debug("create path:{}", child)
              inQueue((child, monitor.patterns.slice(1, monitor.patterns.length), false, true))
            } else {
              logger.debug(s"ignore modify event for :$child")
            }
          /*新增正则,匹配了目录时*/
          case ENTRY_CREATE if model == ListenModel.REGEX =>
            val child = LocalFileWrapper(Paths.get(monitor.path).resolve(name), source.encoding)
            logger.debug("create path:{}", child)
            inQueue((child, monitor.patterns.slice(1, monitor.patterns.length), false, true))

          /*文件修改*/
          case ENTRY_MODIFY if model == ListenModel.FILE =>
            val child = LocalFileWrapper(Paths.get(monitor.path).getParent.resolve(name), source.encoding)
            if (getFileWrapper(monitor.path).absolutePath == child.absolutePath) {
              logger.debug(s"process modify file:$child")
              inQueue((child, Array[Pattern](), false, false))
            } else {
              logger.debug(s"ignore modify event for:$child")
            }
          case ENTRY_MODIFY if model == ListenModel.REGEX && monitor.patterns.nonEmpty =>
            val child = LocalFileWrapper(Paths.get(monitor.path).resolve(name), source.encoding)
            if (wildMatch(monitor.patterns(0), child.name) && !child.isDirectory) {
              logger.debug(s"process modify file:$child")
              inQueue((child, monitor.patterns.slice(1, monitor.patterns.length), false, false))
            } else {
              logger.debug(s"ignore modify event for :$child")
            }
          case ENTRY_MODIFY if model == ListenModel.REGEX =>
            val child = LocalFileWrapper(Paths.get(monitor.path).resolve(name), source.encoding)
            if (!child.isDirectory) {
              inQueue((child, monitor.patterns, false, false))
            } else {
              logger.debug(s"ignore modify event for :$child")
            }
          case ENTRY_MODIFY if model == ListenModel.DICTIONARY =>
            val child = LocalFileWrapper(Paths.get(monitor.path).resolve(name), source.encoding)
            if (!child.isDirectory) {
              logger.debug(s"process modify file:$child")
              inQueue((child, Array[Pattern](), false, false))
            } else {
              logger.debug(s"ignore modify event for :$child")
            }
          case obj =>
            logger.debug(s"monitor ignore event:$obj")

        }

      }

      override def receive: Receive = {


        /*处理一个事件*/
        case (key: WatchKey, events: List[WatchEvent[_]@unchecked]) =>

          if (done.get()) {
            context.stop(self)
          } else {
            events.foreach(event => {
              watchKeys.get(key) match {
                case Some(monitor) =>
                  process(event, monitor)
                case _ =>
              }
            })
          }


      }
    }


    override def preStart(): Unit = {
      context.watch(listener)
    }

    @SuppressWarnings(Array("unchecked"))
    private[monitor] def cast(event: WatchEvent[_]): WatchEvent[Path] = event.asInstanceOf[WatchEvent[Path]]

    private[monitor] lazy val processor = context.actorOf(Props.create(classOf[EventProcessor], this).withDispatcher("akka.dispatcher.processor"))

    private def close(): Unit = {
      context.unwatch(listener)
      context.children.foreach(context.stop)

      context.stop(self)
    }

    override def receive: Receive = {
      case Terminated(_) =>
        close()

      case Opt.WATCH =>
        if (done.get()) {
          close()
        } else {
          try {
            watcher.poll(WAIT_TIME * 10, TimeUnit.MILLISECONDS) match {
              case null =>
              case key: WatchKey if !done.get() =>
                import scala.collection.JavaConversions._
                val events = key.pollEvents().toList
                processor ! (key, events)
                key.reset()
            }

          } catch {
            case e: InterruptedException =>
              logger.debug(s"ignore monitor poll timeout error:${e.getCause}")
            case e: ClosedWatchServiceException =>

              logger.debug(s"ignore monitor closed error:${e.getCause}")
            case e: Exception =>
              logger.warn(s"monitor error:${e.getMessage}")
          }
          if (done.get()) {
            close()
          } else {
            self ! Opt.WATCH
            /*context.system.scheduler.scheduleOnce(WAIT_TIME microsecond){
               if(!done.get())  {

               }
            }*/

          }

        }


    }
  }

}
