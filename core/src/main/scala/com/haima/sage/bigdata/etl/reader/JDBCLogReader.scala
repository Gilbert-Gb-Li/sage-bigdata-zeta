package com.haima.sage.bigdata.etl.reader

import java.text.{DateFormat, SimpleDateFormat}
import java.util
import java.util.Date

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.{JDBCSource, ReadPosition}
import com.haima.sage.bigdata.etl.stream.JDBCStream

/**
  * Created by zhhuiyan on 14/12/26.
  */
class JDBCLogReader(conf: JDBCSource, override val position: ReadPosition) extends LogReader[RichMap] with Position {
  private final val sdf = new ThreadLocal[DateFormat]() {
    protected override def initialValue(): DateFormat = {
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    }
  }


  val column: String = conf.column.replaceAll("`","")
  override val stream = JDBCStream(conf)
  override val iterator: Iterator[RichMap] = new Iterator[RichMap] {
    override def next(): RichMap = {
      val event = stream.next()
      val org: Option[Any] = column.split("\\.").toList match {
        case head :: Nil =>
          event.get(head) match {
            case None =>
              val list = event.map {
                /*表名,对应的数据*/
                case (key, v) =>
                  v match {
                    case data: util.Map[String@unchecked, Any@unchecked] =>
                      data.get(head)
                    case _ =>
                      null
                  }
              }.filterNot(_ == null)
              list.toList match {
                  /*只获取第一条,应该也只有一条*/
                case h :: _ =>
                  Some(h)
                case Nil =>
                  None
              }
            case obj =>
              obj
          }
        case tab :: col :: _ =>
          event.get(tab) match {

            case Some(data: java.util.Map[String@unchecked, Any@unchecked]) =>
              data.get(col) match {
                case null =>
                  null
                case obj =>
                  Some(obj)
              }
            case _ =>
              None
          }
        case _ =>
          None
      }
      val value: Long = org match {
        case Some(date: java.sql.Date) =>
          date.getTime
        case Some(date: Date) =>
          date.getTime
        case Some(data: Long) =>
          data
        case Some(data: Int) =>
          data
        case Some(data: Double) =>
          data.toLong
        case Some(data: Float) =>
          data.toLong
        case Some(data: BigDecimal) =>
          data.longValue()
        case Some(data: java.math.BigDecimal) =>
          data.longValue()
        case Some(data: String) if data.matches( """[\d]+""") =>
          data.toLong
        case Some(data: String) if data.matches("\\d{4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}") =>
          sdf.get().parse(data).getTime
        case Some(x) =>
          throw new UnsupportedOperationException(s"column($column}) unsupport type for data:[$x ,${x.getClass}]")
        case None =>
          throw new UnsupportedOperationException(s"unknown column($column}) for index data")
      }
      position.recordIncrement()
      position.setPosition(value)
      event
    }

    override def hasNext: Boolean = stream.hasNext
  }

  def hasError: (Boolean, String) = {
    stream.msg match {
      case "" =>
        (false, null)
      case msg =>
        (true, msg)
    }

  }

  def skip(skip: Long): Long = 0

  override def path: String = conf.uri

  /*val position: ReadPosition = {
    ReadPosition(path, 0, conf.start.toString.toLong)
  }*/

}
