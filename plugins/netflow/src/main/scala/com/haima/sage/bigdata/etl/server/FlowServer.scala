package com.haima.sage.bigdata.etl.server

import akka.actor.Actor
import com.haima.sage.bigdata.etl.utils.Logger
import io.netflow.flows.{cflow, sflow}
import io.netflow.lib.{FlowPacket, NetFlow, SFlow}

import scala.util.Try

/**
  * Created by zhhuiyan on 16/4/25.
  */
class FlowServer extends Actor with TemplateProc[cflow.Template] with Logger {

  private var cache: Map[Int, cflow.Template] = Map()

  def templates = cache

  def setTemplate(template: cflow.Template): Unit = {
    cache += (template.number -> template)
  }

  override def receive: Receive = {
    case NetFlow(sender, buf) =>
      val handled: Option[FlowPacket] = {
        logger.debug(s"${buf.toString()}")


        Try {
          buf.getUnsignedShort(0)
        }.toOption match {
          case Some(1) =>
            val try1 = cflow.NetFlowV1Packet(sender, buf)
            try1.toOption
          case Some(5) => cflow.NetFlowV5Packet(sender, buf).toOption
          case Some(6) => cflow.NetFlowV6Packet(sender, buf).toOption
          case Some(7) => cflow.NetFlowV7Packet(sender, buf).toOption
          case Some(9) => cflow.NetFlowV9Packet(sender, buf, this).toOption
          case Some(10) =>
            logger.info("We do not handle NetFlow IPFIX yet")
            //Some(cflow.NetFlowV10Packet(sender, buf))
            None

          case _ => None
        }
      }
      buf.release()
      context.parent ! handled.map(_.flows.map(_.toMap))

    case SFlow(sender, buf) =>
      if (buf.readableBytes < 28) {
        logger.warn("Unable to parse FlowPacket")
        //FIXME FlowManager.bad(sender)
        context.parent ! None
      } else {
        val handled: Option[FlowPacket] = {
          Try(buf.getLong(0)).toOption match {
            case Some(3) =>
              logger.info("We do not handle sFlow v3 yet"); None // sFlow 3
            case Some(4) =>
              logger.info("We do not handle sFlow v4 yet"); None // sFlow 4
            case Some(5) =>
              sflow.SFlowV5Packet(sender, buf)
              logger.info("We do not handle sFlow v5 yet")
              None // sFlow 5
            case _ => None
          }
        }
        context.parent ! handled
      }
      buf.release()
    case obj =>
      logger.warn(s"Unknown msg:$obj")
  }
}

trait TemplateProc[T] {
  def templates :Map[Int, T]

  def setTemplate(template: T): Unit
}
