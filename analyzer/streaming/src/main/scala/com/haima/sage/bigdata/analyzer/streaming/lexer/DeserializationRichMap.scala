package com.haima.sage.bigdata.analyzer.streaming.lexer

import com.haima.sage.bigdata.etl.common.model.RichMap
import org.apache.flink.api.common.serialization.DeserializationSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._

case class DeserializationRichMap(encoding: Option[String]=None) extends DeserializationSchema[RichMap] {
  override def deserialize(bytes: Array[Byte]): RichMap = {
    encoding match {
      case Some(enc) if enc.length > 0 =>
        RichMap(Map("raw" -> new String(bytes, enc)))
      case _ =>
        RichMap(Map("raw" -> new String(bytes)))
    }
  }

  override def isEndOfStream(t: RichMap): Boolean = {
    t.isEmpty
  }

  override def getProducedType: TypeInformation[RichMap] = createTypeInformation[RichMap]
}
