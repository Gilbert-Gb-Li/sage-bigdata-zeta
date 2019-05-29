package com.haima.sage.bigdata.etl.stream

import com.haima.sage.bigdata.etl.common.model.{RichMap, Stream}
import org.apache.hadoop.hive.ql.exec.vector._
import org.apache.orc.RecordReader

case class OrcStream(rows: RecordReader, batch: VectorizedRowBatch, fields: List[String]) extends Stream[RichMap](None) {


  var index = 0

  override def hasNext: Boolean = {
    if (isRunning) {
      if (batch.size > 0 && index < batch.size) {
        true
      } else {
        index = 0
        rows.nextBatch(batch)
      }
    } else {
      false
    }


  }


  override def next(): RichMap = {
    val rt = fields.zip(batch.cols map (data => {
      val stringBuffer = new java.lang.StringBuilder()
      /*
      * FIXME get real data type
      * */
      data.stringifyValue(stringBuffer, index)
      stringBuffer
    }))
    index = index + 1
    RichMap(rt.toMap)
  }

  override def close(): Unit = {
    rows.close()
    super.close()

  }
}
