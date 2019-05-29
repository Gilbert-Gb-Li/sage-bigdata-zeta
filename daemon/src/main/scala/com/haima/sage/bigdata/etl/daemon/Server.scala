package com.haima.sage.bigdata.etl.daemon

import java.io._

import akka.actor._
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.utils.Logger

import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class Server() extends Actor with Logger {

  import  context.dispatcher
  import sys.process._

  //sendIdentifyRequest()
  logger.info("local daemon is started!")

  /*def remote: ActorRef = context.actorOf(ClusterClient.props(ClusterClientSettings(context.system).withInitialContacts(masters)))


  def sendIdentifyRequest(): Unit = {
    remote ! ClusterClient.Send("/user/server", Identify("daemon"), localAffinity = true)
  }*/

  def checkWork(work: String, path: String): Unit = {
    context.actorSelection(ActorPath.fromString(work)) ! Identify(path)
  }

  def restart(work: String, path: String): Unit = {
    self ! (path, Opt.RESTART)
    context.system.scheduler.scheduleOnce(1 seconds) {
      checkWork(work, path)
    }

  }

  val props = new java.util.Properties()
  var actors: Map[ActorRef, String] = Map[ActorRef, String]()

  val paths: Array[String] = this.getClass.getResource("/").getPath.split("/")

  val file_path: String = if (paths.last.contains("classes")) paths.slice(0, paths.length - 3).mkString("/") else paths.slice(0, paths.length - 1).mkString("/")


  val file = new File(s"$file_path/db/daemon-store.properties")


  override def preStart(): Unit = {

    if (!file.exists) {
      val parent = new File(s"$file_path/db/")
      parent.mkdir()

      logger.info(s"local daemon in $file_path")
      file.createNewFile()
    }
    val inputStream = file.inputStream()
    props.load(inputStream)
    inputStream.close()

    import scala.collection.JavaConversions._
    /*
    * 当daemon启动时,认为是宕机重启服务,启动监听的worker
    * */
    props.values.foreach(d=>start(d.toString))
  }


  import scala.collection.JavaConversions._

  def receive: Receive = {
    case Terminated(actor) if actors.contains(actor) =>
      context.unwatch(actor)
      logger.error(s"Detected worker[${actor.path}] unreachable.")
      actors.get(actor).foreach {
        key =>
          start(props.getProperty(key))
      }


    case ActorIdentity(path, None) if props.values().toSet.contains(path) =>
      logger.warn(s"worker[$path] not available")

      props.find(_._2 == path) match {
        case Some((key, value)) =>
          restart(key, props.getProperty(value))
        case _ =>
          logger.warn(s"worker[$path] not conf ed. ")
      }
    case ActorIdentity(path, Some(actor)) =>
      logger.info(s" listen on work $path ")
      context.watch(actor)
    case (Opt.REGISTER, worker: String, path: String) =>
      logger.debug(s" add a worker[$worker] with path[$path]")
      context.watch(sender())
      val stream = file.outputStream()
      actors += (sender() -> worker)
      props.setProperty(worker, path)
      props.store(stream, "Copyright (c) SAGE 2017 ")
      stream.close()

      sender() ! ("daemon", "received")
    case (Opt.UN_REGISTER, worker: String) =>
      logger.debug(s" remove a worker[$worker]")
      actors = actors.filter(_._2 != worker)
      context.unwatch(sender())
      props.remove(worker)
      val output = file.outputStream()
      props.store(output, "Copyright (c) SAGE 2017 ")
      output.close()
    case (worker: String, Opt.START) =>
      val path = workerPath(worker)
      sender() ! start(path)

    case (worker: String, Opt.STOP) =>
      val path = workerPath(worker)
      actors.find(_._2 == worker).foreach(actor => context.unwatch(actor._1))
      actors = actors.filter(_._2 != worker)
      context.unwatch(sender())
      sender() ! stop(path)

    case (remote: ActorRef, path: String, Opt.RESTART) =>
      //if (stop(path).message.endsWith("to stop")) {
      if (stop(path).param.head.toString.endsWith("to stop")) {
        remote ! start(path)
      } else {
        context.system.scheduler.scheduleOnce(5 second, self, (remote, Opt.RESTART))
      }
    case (worker: String, Opt.RESTART) =>
      val path = workerPath(worker)
      //if (stop(path).message.endsWith("to stop")) {
      if (stop(path).param.head.toString.endsWith("to stop")) {
        sender() ! start(path)
      } else {
        context.system.scheduler.scheduleOnce(5 second, self, (sender(), path, Opt.RESTART))
      }

    case msg =>
      logger.warn(s"unknown message:$msg")
  }

  def workerPath(worker: String): String = {
    props.getProperty(worker)

  }

  def start(path: String): BuildResult = {

    if (System.getProperty("os.name").toLowerCase.indexOf("win") != -1) {
      val execstring = s"$path/bin/start-worker.bat"
      logger.debug(s"start server in $execstring")
      context.actorOf(Props(classOf[ExecActor], this, execstring)) ! Opt.START
      //Result("200", "cmd is received please waiting")
      BuildResult("200","928", "cmd is received please waiting")
    } else {
      val execstring = s"$path/bin/start-worker.sh"
      logger.debug(s"start server in $execstring")
      Try(execstring.lineStream_!.reduce((left, right) => left + right)) match {
        case Success(msg) =>
          //Result("200", "start worker server success")
          BuildResult("200", "928", "start worker server success")
        case Failure(e) =>
          logger.error(s"start worker server error: "+e.getMessage)
          //Result("304", "start worker server error")
          BuildResult("304", "928", "start worker server error")
      }
    } // treate other system as *nix

  }

  class ExecActor(exec: String) extends Actor {
    synchronized (this)
    override def receive: Receive = {
      case Opt.START =>
        Try(exec.lineStream_!.reduce((left, right) => left + right)) match {
          case Success(msg) =>
            //sender() ! Result("200", msg)
            sender() ! BuildResult("200","928", msg)
            context.stop(self)
          case Failure(e) =>
            //sender() ! Result("304", e.getMessage)
            sender() ! BuildResult("304", "901", e.getMessage)
            context.stop(self)
        }
      case _=>
        context.stop(self)
    }
  }

  def stop(path: String): BuildResult = {
    val execstring = if (System.getProperty("os.name").toLowerCase.indexOf("win") != -1)
      s"$path/bin/stop-worker.bat"
    else // treate other system as *nix
      s"$path/bin/stop-worker.sh"

    logger.debug(s"stop server in $execstring ")
    Try(execstring.lineStream_!.reduce((left, right) => left + right)) match {
      case Success(msg) =>
        //Result("200", "stop worker server success")
        BuildResult("200", "928", "stop worker server success")
      case Failure(e) =>
        logger.error(s"stop worker server error: "+e.getMessage)
        //Result("304", "stop worker server error")
        BuildResult("304", "928", "stop worker server error")


    }

  }
}

