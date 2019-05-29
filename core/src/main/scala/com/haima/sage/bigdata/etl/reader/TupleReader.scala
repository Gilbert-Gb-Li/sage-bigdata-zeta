package com.haima.sage.bigdata.etl.reader

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.base.LogReader
import com.haima.sage.bigdata.etl.common.model.Stream


/**
  * 计算两个流进行合并，数据流first 要求数据发生早于 数据流second ，切数据流second 必须能[0,1]对一的匹配上数据流first
  */
class TupleReader(first: LogReader[RichMap],
                  second: LogReader[RichMap],
                  orders: (String, String),
                  by: (String, String),
                  cache: Int
                 ) extends LogReader[RichMap] {


  override def skip(skip: Long): Long = 0

  override def path: String = s"tuple-${first.path}-${first.path}"

  final var firstCache: List[Map[String, Any]] = List()
  final var secondCache: List[Map[String, Any]] = List()

  override val stream: Stream[RichMap] = new Stream[RichMap](None) {
    var item: Map[String, Any] = _

    override def next(): RichMap = {
      first.stream.next()
    }

    override def hasNext: Boolean = {
      state match {
        case State.ready =>
          true
        case State.init =>
          if (first.isClosed || second.isClosed) {
            false
          } else {

          }
          true
        case _ =>
          false
      }

    }

    def make(first: Stream[RichMap], second: Stream[RichMap]): Map[String, Any] = {
      if (first.hasNext) {
        item = first.next()
      } else {
        throw new RuntimeException("tuple1 had close")
      }
      item.get(by._1) match {
        case Some(first_data) =>
          var flag = true

          if (secondCache.nonEmpty)
            secondCache = secondCache.dropWhile(temp => {
              if (temp != null && temp.get(by._2).orNull == first_data) {
                item = item ++ temp
                flag = false
                true
              } else {
                false
              }
            })


          while (flag && secondCache.size < cache) {
            if (second.hasNext) {
              val temp = second.next()
              if (temp != null && temp.get(by._2).isDefined) {
                if (temp.get(by._2).orNull == item.get(by._1)) {
                  item = item ++ temp
                  flag = false
                } else {
                  secondCache = temp :: secondCache

                }
              }
            } else {
              throw new RuntimeException("tuple2 had close")
            }

          }
          item


        case _ =>
          item
      }


    }

    override def close(): Unit = {
      super.close()
      first.close()
      second.close()
    }

  }

}
