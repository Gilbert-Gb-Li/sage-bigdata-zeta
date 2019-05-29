package com.haima.sage.bigdata.etl.codec

import java.io.IOException

import com.haima.sage.bigdata.etl.common.model.{Event, Stream}

/**
  * Created by zhhuiyan on 2014/11/5.
  */
object LineStream {
  def apply(codec: LineCodec, reader: Stream[Event]) = new LineStream(codec, reader)
}

class LineStream(codec: LineCodec, reader: Stream[Event]) extends Stream[Event](codec.filter) {

  def hasNext: Boolean = {
    reader.hasNext

  }

  @throws(classOf[IOException])
  override def close() {
    reader.close()
    super.close()
  }

  override def next(): Event = reader.next()
}