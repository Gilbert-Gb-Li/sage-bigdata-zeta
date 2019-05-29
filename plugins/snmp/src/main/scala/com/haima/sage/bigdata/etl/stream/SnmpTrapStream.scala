package com.haima.sage.bigdata.etl.stream

import java.io.IOException
import java.util.concurrent.TimeUnit

import com.haima.sage.bigdata.etl.common.Constants.Constants
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.utils.Mapper
import net.percederberg.mibble.value.ObjectIdentifierValue
import org.snmp4j._
import org.snmp4j.mp.SnmpConstants
import org.snmp4j.smi.{OID, VariableBinding}

import scala.annotation.tailrec

/**
  * Created by zhhuiyan on 2014/11/5.
  */
object SnmpTrapStream {
  def apply(snmp: Snmp, _timeout: Long) = new SnmpTrapStream(snmp, _timeout)

  class SnmpTrapStream(snmp: Snmp, _timeout: Long) extends QueueStream[RichMap](None, _timeout) with Mapper {

    import net.percederberg.mibble.MibLoader

    private[SnmpTrapStream] lazy val staticNames = {
      val clazz = classOf[SnmpConstants]
      val fields = clazz.getDeclaredFields

      fields.filter(_.getType.getCanonicalName == classOf[OID].getCanonicalName).map(field => {

        (field.get(clazz).toString, field.getName)
      }).toMap
    }


    val finder: ObjectIdentifierValue = {
      val classLoader: ClassLoader = classOf[Constants].getClassLoader

      val CLASSPATH = classLoader.getClass.getResource("/").getPath


      /*加载mib文件*/
      logger.debug(s"mibs:${CLASSPATH}mibs")

      implicit val loader = new MibLoader
      load(new java.io.File(s"${CLASSPATH}mibs"))
      loader.getRootOid
    }

    private[SnmpTrapStream] def locateNameByOid(oid: String): String = {
      staticNames.get(oid) match {
        case Some(v) =>
          v
        case _ =>
          finder.find(oid) match {
            case null =>
              oid
            case data: ObjectIdentifierValue if data.getSymbol.getOid.toString == oid.substring(0, oid.lastIndexOf(".")) && oid.substring(oid.lastIndexOf(".") + 1, oid.length) == "0" =>
              data.getSymbol.getName
            case _ =>
              oid
          }
      }
    }

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

    var count = 0l
    snmp.addCommandResponder(action((event: CommandResponderEvent) => {

      import scala.collection.JavaConversions._
      def trans(recVBs: java.util.Vector[_ <: VariableBinding]): Map[String, Any] = {
        recVBs.map(vb => {
          val symbolOid = locateNameByOid(vb.getOid.toString)
          val symbolVal = locateNameByOid(vb.toValueString)
          (symbolOid, symbolVal)
        }).toMap[String, String]

      }

      val command: PDU = event.getPDU
      if (command != null) {

        /*command.getType match {
          case PDU.V1TRAP =>
          case PDU.TRAP =>
          case PDU.NOTIFICATION =>
        }*/

        val recVBs = command.getVariableBindings
        if (recVBs != null) {
          try {
            val data = trans(recVBs)
            logger.debug(s"trap: $data")
            while (!queue.offer(data)) {
              TimeUnit.MILLISECONDS.sleep(_timeout)
            }
          }
          catch {
            case e: Exception =>
              e.printStackTrace()
              logger.error(s" message process error:$e")
          }
        }
      }
    }))


    import net.percederberg.mibble.{MibType, MibTypeTag}

    def isIpAddress(`type`: MibType): Boolean = { // IpAddress ::= [APPLICATION 0]
      `type`.hasTag(MibTypeTag.APPLICATION_CATEGORY, 0)
    }

    def isTimeTicks(`type`: MibType): Boolean = { // TimeTicks ::= [APPLICATION 3]
      `type`.hasTag(MibTypeTag.APPLICATION_CATEGORY, 3)
    }

    @throws(classOf[IOException])
    override def close() {
      super.close()
      snmp.close()
    }


    implicit def action(fun: (CommandResponderEvent) => Unit): CommandResponder =
      new CommandResponder {
        override def processPdu(event: CommandResponderEvent): Unit = fun(event)
      }
  }

}

