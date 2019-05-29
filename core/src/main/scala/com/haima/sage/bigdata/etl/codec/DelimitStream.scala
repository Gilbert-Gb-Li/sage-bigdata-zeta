package com.haima.sage.bigdata.etl.codec

import java.io.IOException

import com.haima.sage.bigdata.etl.common.model.{Event, Stream}

import scala.annotation.tailrec

/**
  * Created by zhhuiyan on 2014/11/5.
  */
object DelimitStream {
  def apply(delimit: DelimitCodec, reader: Stream[Event]) = new DelimitStream(delimit, reader)
}

class DelimitStream(delimit: DelimitCodec, reader: Stream[Event]) extends Stream[Event](delimit.filter) {


  private var cache: Array[String] = Array()
  /*未完成*/
  private var imperfect = true
  private var skip = 0
  private var lineEnd = false

  private var header: Option[Map[String, Any]] = None
  private val split = {
    if (delimit.delimit.equals("\\n")) {
      "\n"
    } else if (delimit.delimit.equals("\\t")) {
      "\t"
    } else if (delimit.delimit.equals("\\s")) {
      " "
    } else if ("|()[]*.?".contains(delimit.delimit)) {
      "\\" + delimit.delimit
    } else {
      delimit.delimit
    }
  }

/*获取数据，并判断最后一条是否完整*/
  def get(): Unit = {
    val it = reader.next()
    cache = it.content.split(split).map(line=>{
      if ("\n".equals(line) || "\u2028".equals(line)
        || "\u2029".equals(line)||"\u0085".equals(line)
        ||"\r".equals(line) ||"\r\n".equals(line) ) line
      else
        line+split
    }
    )
    if (it.content.endsWith(split) || it.content.endsWith(split+"\n") || it.content.endsWith(split+"\u2028")
      ||it.content.endsWith(split+"\u2029")||it.content.endsWith(split+"\u0085")
      ||it.content.endsWith(split+"\r") ||it.content.endsWith(split+"\r\n") ) {
      imperfect = false
    }  else {
      cache = cache.slice(0, cache.length-1) ++ cache.last.split(split)
      imperfect = true
    }
    header = it.header
  }

  private def process(): String = {
    val _cache = if (cache.length > 1) {
      (cache.slice(1, cache.length), cache(0))
    } else if (cache.length == 1) {
      (Array[String](), cache(0))
    } else {
      (Array[String](), "")
    }
    cache = _cache._1
    _cache._2
  }

  final def hasNext: Boolean = {
    cache.length match {
      case 1 if imperfect =>
        lineEnd = true
        val _cache = cache(0)
        if (reader.hasNext) {
          get()
          cache(0) = _cache + cache(0)
          hasNext
        } else {
          if(!isTails(cache(0)))
            skip = skip - split.length
          true
        }
      case 1 if !imperfect && isTails(cache(0))=>
        skip = skip+cache(0).length
        lineEnd = true
        val _cache = ""// cache(0)
        if (reader.hasNext) {
          get()
          cache(0) = _cache + cache(0)
          hasNext
        } else {
          true
        }
      case d if d >= 1 =>
        true
      case _ =>
        if (reader.hasNext) {
          get()
          hasNext
        } else {
          false
        }
    }
  }

  @throws(classOf[IOException])
  override def close() {
    reader.close()
    super.close()

  }

  override def next(): Event = {
    try {

      val _data =  process()
        val data = _data.replaceAll(split,"")
      val ignoreLength=if(lineEnd)  {
        lineEnd = false
        skip+split.length
      }else split.length
      skip=0
      Event( Option(header.getOrElse(Map())+ ("ignoreLength"->ignoreLength)), data)
    }
    catch {
      case e: Exception =>
        logger.error("MK A LINE ERROR:{}", e)
        null
    }


  }

  def isTails(end:String):Boolean={
    "\n".equals(end) || "\u2028".equals(end) ||
      "\u2029".equals(end)||"\u0085".equals(end)  ||
      "\r".equals(end) ||"\r\n".equals(end)
  }
}

