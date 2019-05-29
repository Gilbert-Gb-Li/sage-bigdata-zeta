package com.haima.sage.bigdata.etl.plugin.es_2.reader

import java.util.Date
import javax.activation.UnsupportedDataTypeException

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.{ES2Source, ReadPosition}
import com.haima.sage.bigdata.etl.plugin.es_2.stream.ESStream
import com.haima.sage.bigdata.etl.reader.Position

/**
  * Created by zhhuiyan on 14/12/26.
  */
class ESLogReader(val conf: ES2Source, val value: Any, step: Long,val channelId:String) extends LogReader[RichMap] with Position {

  override val stream = ESStream(conf.cluster, conf.hostPorts, conf.index, conf.esType, conf.field, value, step, conf.timeout)

  def skip(skip: Long): Long = 0

  def path: String = s"$channelId:"+conf.uri

  override val iterator: Iterator[RichMap] = new Iterator[RichMap] {
    override def next(): RichMap = {
      val event = stream.next()
      val value: Long =
        event.get(conf.field) match {
          case Some(date: Date) =>
            date.getTime
          case Some(data: Long) =>
            data
          case Some(data: Int) =>
            data
          case Some(data: String) =>
            if (data.matches( """[\d]+"""))
              data.toLong
            else throw new UnsupportedOperationException(s"give  index data: $data :String cannot as index column")
          case Some(x) =>
            throw new UnsupportedOperationException(s"unknown type for index data:$x ,:${x.getClass}")
          case None =>
            throw new UnsupportedOperationException(s"unknown field(${conf.field}}) for index data")
        }
      position.recordIncrement()
      position.setPosition(value)
      event
    }

    override def hasNext: Boolean = stream.hasNext
  }
  val position = {
    val pos: Long = value match {
      case date: Date =>
        date.getTime
      case v: Long =>
        v
      case v: Int =>
        v
      case v =>
        //throw new UnsupportedDataTypeException(s"data:$v")
        0L
    }
    new ReadPosition(path, 0, pos)
  }

}
