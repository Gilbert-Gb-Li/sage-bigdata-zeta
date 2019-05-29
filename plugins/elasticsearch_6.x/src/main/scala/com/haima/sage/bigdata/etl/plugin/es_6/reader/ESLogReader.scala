package com.haima.sage.bigdata.etl.plugin.es_6.reader

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.{ES6Source, ReadPosition, RichMap}
import com.haima.sage.bigdata.etl.plugin.es_6.stream.ESStream
import com.haima.sage.bigdata.etl.reader.Position
import javax.activation.UnsupportedDataTypeException

/**
  * Created by zhhuiyan on 14/12/26.
  */
class ESLogReader(val conf: ES6Source, val value: Any, step: Long, val channelId: String) extends LogReader[RichMap] with Position {
  private final val format_utc = new ThreadLocal[DateFormat]() {
    protected override def initialValue(): DateFormat = {
      new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.S")
    }
  }
  override val stream = ESStream(conf, value, step)

  def skip(skip: Long): Long = 0

  def path: String = s"$channelId:" + conf.uri

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
  val position: ReadPosition = {
    val pos: Long = value match {
      case date: Date =>
        date.getTime
      case v: Long =>
        v
      case v: Int =>
        v
      case v: String =>
        format_utc.get().parse(v).getTime
      case v =>
        throw new UnsupportedDataTypeException(s"data:$v")
    }
    ReadPosition(path, 0, pos)
  }

}
