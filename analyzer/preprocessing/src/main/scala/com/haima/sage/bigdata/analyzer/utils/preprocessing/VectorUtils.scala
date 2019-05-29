package com.haima.sage.bigdata.analyzer.utils.preprocessing

import java.util.{Calendar, Date}

import com.haima.sage.bigdata.etl.common.model.RichMap
import org.apache.flink.ml.math.{DenseVector, SparseVector, Vector}

object VectorUtils extends Serializable {

  /*
  *
  *时间转向量
  * */
  def date2Vector(date: Date): Vector = {
    val calender = Calendar.getInstance()
    calender.setTime(date)
    calender.get(Calendar.MONTH) //12
    calender.get(Calendar.WEEK_OF_MONTH) //5
    calender.get(Calendar.DAY_OF_WEEK) //7
    calender.get(Calendar.HOUR_OF_DAY) //24
    calender.get(Calendar.MINUTE) //60
    SparseVector(12 + 7 + 5 + 24 + 60,
      Array(calender.get(Calendar.MONTH),
        12 + calender.get(Calendar.WEEK_OF_MONTH),
        5 + 12 + calender.get(Calendar.DAY_OF_WEEK),
        5 + 12 + 7 + calender.get(Calendar.HOUR_OF_DAY),
        5 + 12 + 7 + 24 + calender.get(Calendar.MINUTE)
      )
      , Array(1, 1, 1, 1, 1))
  }

  def mergeVector: (Vector, Vector) => Vector = {
    case (c: SparseVector, d1@SparseVector(s, indices, _data)) =>
      if (c.size == 0) {
        d1
      } else {
        val max = c.size
        SparseVector(s + c.size, c.indices ++ indices.map(_ + max), c.data ++ _data)
      }
    case (c: SparseVector, DenseVector(_data)) =>
      if (c.size == 0) {
        if (_data.length == 0) {
          c
        } else {
          SparseVector.fromCOO(_data.length, _data.zipWithIndex.map(_.swap))
        }

      } else {

        if (_data.length == 0) {
          c
        } else {
          val max = c.size
          SparseVector(c.size + _data.length, c.indices ++ _data.indices.map(_ + max),
            c.data ++ _data)
        }
      }
    case (DenseVector(data), d1@SparseVector(s, indices, _data)) =>
      if (data.length == 0) {
        d1
      } else {
        val max = data.length
        SparseVector(s + data.length, data.indices.toArray ++ indices.map(_ + max), data ++ _data)
      }
    case (DenseVector(data), DenseVector(_data)) =>
      DenseVector(data ++ _data)

  }

  def add(data: RichMap, vector: Vector): RichMap = {
    data.get("vector") match {
      case Some(d: Vector) =>
        data + ("vector" -> mergeVector(d, vector))
      case _ =>
        data + ("vector" -> vector)
    }
  }

}
