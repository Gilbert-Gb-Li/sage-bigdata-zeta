package com.haima.sage.bigdata.etl.monitor.file

import java.util.concurrent.LinkedBlockingQueue

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.{Envelope, MailboxType, MessageQueue, ProducesMessageQueue}
import com.haima.sage.bigdata.etl.common.Constants
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

trait LocalFileMessageQueueSemantics

object LocalFileMailbox {

  private lazy val size = Constants.CONF.getLong("app-dispatcher-mailbox.mailbox-size")

  // This is the MessageQueue implementation
  class LocalFileQueue extends MessageQueue
    with LocalFileMessageQueueSemantics {
    private lazy val logger = LoggerFactory.getLogger(classOf[LocalFileQueue])

    /*
    * , new Comparator[Envelope] {
      override def compare(x: Envelope, y: Envelope): Int = {
        if (x == null || y == null) {
          -1
        } else {
          (x.message, y.message) match {
            case ((xFile: LocalFileWrapper, _, _, _), (yFile: LocalFileWrapper, _, _, _)) =>
              if (xFile.isDirectory) {
                -1
              } else if (yFile.isDirectory) {
                1
              } else {
                0
              }
            case _ =>
              -1
          }
        }

      }
    }
    *
    * */
    private final val files = new LinkedBlockingQueue[Envelope]()
    private final val dirs = new LinkedBlockingQueue[Envelope]()

    // these should be implemented; queue used as example
    def enqueue(receiver: ActorRef, handle: Envelope): Unit = {
      handle.message match {
        case (file: LocalFileWrapper, _, _, _) if file.isDirectory =>
          dirs.put(handle)
        case (_: LocalFileWrapper, _, _, _) =>
          files.put(handle)
          if (files.size() > size) {
            files.poll()
          }

        case _ =>

          logger.debug("unknown msg for this {}", handle.message)
      }

      //  queue.put(handle)
      /*handle.message match {
        case (file: LocalFileWrapper, _, _, _) =>
          /*if (queue.nonEmpty)
            queue.dequeueFirst(_.message match {
              case (_file: LocalFileWrapper, _, _, _) =>
                file.absolutePath == _file.absolutePath
              case _ =>
                false
            })*/

          /*if (numberOfMessages > 100000) {
            val old = queue.take()
            logger.warn(s"file queue is full size[100000],drop  the old ${old.message}")
          }*/

        case _ =>

          logger.debug("unknown msg for this {}", handle.message)
      }*/

    }

    def dequeue(): Envelope = if (dirs.isEmpty) files.poll() else dirs.poll()

    def numberOfMessages: Int = dirs.size + files.size()

    def hasMessages: Boolean = if (dirs.isEmpty) !files.isEmpty else true

    def cleanUp(owner: ActorRef, deadLetters: MessageQueue) {
      dirs.clear()
      files.clear()
      /*while (hasMessages) {
        deadLetters.enqueue(owner, dequeue())
      }*/
    }
  }

}

// This is the Mailbox implementation
class LocalFileMailbox extends MailboxType
  with ProducesMessageQueue[LocalFileMailbox.LocalFileQueue] {

  import LocalFileMailbox._

  // This constructor signature must exist, it will be called by Akka
  def this(settings: ActorSystem.Settings, config: Config) = {
    // put your initialization code here
    this()
  }

  // The create method is called to create the MessageQueue
  final override def create(
                             owner: Option[ActorRef],
                             system: Option[ActorSystem]): MessageQueue =
    new LocalFileQueue()
}