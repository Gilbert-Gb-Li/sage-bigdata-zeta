package com.haima.sage.bigdata.etl.stream

import java.io.IOException
import java.util.concurrent.{TimeUnit, TimeoutException}

import com.haima.sage.bigdata.etl.common.Constants.Constants
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.utils.Mapper
import net.percederberg.mibble.value.ObjectIdentifierValue
import org.snmp4j._
import org.snmp4j.mp.SnmpConstants
import com.haima.sage.bigdata.etl.common.model.{Event, Stream}
import org.snmp4j.event.ResponseEvent
import org.snmp4j.smi.{Integer32, Null, OID, VariableBinding}

import scala.collection.JavaConversions._
import scala.annotation.tailrec

/**
  * Created by zhhuiyan on 2014/11/5.
  */
object SnmpWalkStream {
  def apply(snmp: Snmp, target: CommunityTarget, targetOid: String, _timeout: Long) = new SnmpWalkStream(snmp, target, targetOid, _timeout)

  class SnmpWalkStream(snmp: Snmp, target: CommunityTarget, targetOid: String, _timeout: Long) extends Stream[RichMap](None) with Mapper {


    import net.percederberg.mibble.MibLoader

    private[SnmpWalkStream] lazy val staticNames = {
      val clazz = classOf[SnmpConstants]
      val fields = clazz.getDeclaredFields

      fields.filter(_.getType.getCanonicalName == classOf[OID].getCanonicalName).map(field => {

        (field.get(clazz).toString, field.getName)
      }).toMap
    }

    /* 获取mib文件路径，并调用load载入mib文件 */
    val finder: ObjectIdentifierValue = {
      val classLoader: ClassLoader = classOf[Constants].getClassLoader
      val CLASSPATH = classLoader.getClass.getResource("/").getPath

      /*加载mib文件*/
      logger.debug(s"mibs:${CLASSPATH}mibs")

      implicit val loader = new MibLoader
      load(new java.io.File(s"${CLASSPATH}mibs"))
      loader.getRootOid
    }

    /* 根据传入的oid获取，匹配mib信息，获取对应的结果，为匹配成功的返回oid */
    private[SnmpWalkStream] def locateNameByOid(oid: String): String = {
      staticNames.get(oid) match {
        case Some(v) =>
          v
        case _ =>
          finder.find(oid) match {
            case null =>
              oid
            case data: ObjectIdentifierValue if data.getSymbol.getOid.toString == oid.substring(0, oid.lastIndexOf(".")) =>
              data.getSymbol.getName
            case _ =>
              oid
          }
      }
    }

    /* 加载mib文件 */
    @tailrec
    private def load(file: java.io.File, brother: List[java.io.File] = List())(implicit loader: MibLoader): Unit = {
      if (file.isFile) {
        loader.load(file)
      } else if (file.isDirectory) {
        val files = file.listFiles().toList
        if (brother.isEmpty) {
          if (files.nonEmpty)
            load(files.head, files.slice(1, files.size))
        } else {
          load(brother.head, brother.slice(1, files.size) ++ files)
        }

      }
    }

    /* 解析mib编码 */
    def trans(vb: VariableBinding): Tuple2[String, Any] = {
      val symbolOid = locateNameByOid(vb.getOid.toString)
      val symbolVal = locateNameByOid(vb.toValueString)
      (symbolOid, symbolVal)
    }

    /* 校验snmp是否完成数据获取 */
    def checkWalkFinished(targetOID: OID, pdu: PDU, vb: VariableBinding): Boolean = {
      var finished: Boolean = false
      if (pdu.getErrorStatus != 0) {
        println("[true] responsePDU.getErrorStatus() != 0 ")
        println(pdu.getErrorStatusText)
        finished = true
      } else if (vb.getOid == null) {
        println("[true] vb.getOid() == null")
        finished = true
      } else if (vb.getOid.size < targetOID.size) {
        println("[true] vb.getOid().size() < targetOID.size()")
        finished = true
      } else if (targetOID.leftMostCompare(targetOID.size, vb.getOid) != 0) {
        println("[true] targetOID.leftMostCompare() != 0")
        finished = true
      } else if (Null.isExceptionSyntax(vb.getVariable.getSyntax)) {
        println("[true] Null.isExceptionSyntax(vb.getVariable().getSyntax())")
        finished = true
      } else if (vb.getOid.compareTo(targetOID) <= 0) {
        println("[true] Variable received is not " + "lexicographic successor of requested " + "one:")
        println(vb.toString + " <= " + targetOID)
        finished = true
      }
      return finished
    }

    /*获取snmp数据*/
    val pdu: PDU = new PDU
    val targetOID: OID = new OID(targetOid)
    pdu.add(new VariableBinding(targetOID))

    var item: RichMap = _

    final def hasNext: Boolean = {
      state match {
        case State.done | State.fail =>
          false
        case State.init =>
          //TODO
          item = RichMap()
          var finished: Boolean = false
          while (!finished) {
            var vb: VariableBinding = null
            val respEvent: ResponseEvent = snmp.getNext(pdu, target)
            val response: PDU = respEvent.getResponse
            if (null == response) {
              throw new TimeoutException("Connect the SNMP service timeout")
            }
            vb = response.get(0)
            // check finish
            finished = checkWalkFinished(targetOID, pdu, vb)
            if (!finished) {
              val data = trans(vb)
              item = item.+(data._1 -> data._2)
              pdu.setRequestID(new Integer32(0))
              pdu.set(0, vb)
            } else {
              System.out.println("SNMP walk OID has finished.")
            }
          }

          //  channel.basicAck(position.position, false)
          state = State.ready
          true
        case State.ready =>
          true
      }
    }

    override def next():  RichMap = {
      state = State.init
      item
    }


    import net.percederberg.mibble.{MibType, MibTypeTag}

    def isIpAddress(`type`: MibType): Boolean = { // IpAddress ::= [APPLICATION 0]
      `type`.hasTag(MibTypeTag.APPLICATION_CATEGORY, 0)
    }

    def isTimeTicks(`type`: MibType): Boolean = { // TimeTicks ::= [APPLICATION 3]
      `type`.hasTag(MibTypeTag.APPLICATION_CATEGORY, 3)
    }

    @throws(classOf[IOException])
    override def close() {
      snmp.close()
    }

    implicit def action(fun: (CommandResponderEvent) => Unit): CommandResponder =
      new CommandResponder {
        override def processPdu(event: CommandResponderEvent): Unit = fun(event)
      }
  }

}

