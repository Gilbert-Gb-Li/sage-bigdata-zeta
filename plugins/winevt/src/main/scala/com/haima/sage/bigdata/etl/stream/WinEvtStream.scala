package com.haima.sage.bigdata.etl.stream

import java.io.IOException

import com.haima.sage.bigdata.etl.common.model.WinEvt.{EvtQueryFilePath, EvtRenderEventXml, INSTANCE}
import com.haima.sage.bigdata.etl.common.model.{Event, Stream, WinEvt}
import com.sun.jna.platform.win32.WinDef

import scala.annotation.tailrec

/**
  * Created by zhhuiyan on 2014/11/5.
  */
object WinEvtStream {
  def apply(path: String) = new WinEvtStream(path)

  class WinEvtStream(path: String) extends Stream[Event](None) {
    private var item: Event = _

    private[WinEvtStream] val evt = INSTANCE
    private val handle = evt.EvtQuery(null, path, null, EvtQueryFilePath)
    private var events: Array[WinEvt.EVT_HANDLE] = _
    private var returned: Array[WinDef.DWORD] = _
    private var index: Int = 0

    private[stream] def eventContent(events: WinEvt.EVT_HANDLE): Option[Array[Byte]] = {
      var bufferSize: WinDef.DWORD = new WinDef.DWORD
      val usedBuffer: WinDef.DWORDByReference = new WinDef.DWORDByReference
      val propertyCount: WinDef.DWORDByReference = new WinDef.DWORDByReference
      val flag: Boolean = evt.EvtRender(null, events, EvtRenderEventXml, bufferSize, null, usedBuffer, propertyCount)
      if (!flag) {
        bufferSize = usedBuffer.getValue
        val content: Array[Byte] = new Array[Byte](bufferSize.intValue)
        evt.EvtRender(null, events, EvtRenderEventXml, bufferSize, content, usedBuffer, propertyCount)
        Some((0 to content.length).zip(content).filter(_._1 % 2 == 0).map(_._2).toArray)
      } else {
        None
      }
    }

    @tailrec
    final def hasNext: Boolean = {
      state match {
        case State.ready =>
          true
        case State.done =>
          false
        case State.fail =>
          false
        case _ =>
          makeData()
          hasNext
      }
    }

    def makeData() = {

      val size: Int = 1
      if (events == null) {
        events = new Array[WinEvt.EVT_HANDLE](size)
        returned = new Array[WinDef.DWORD](size)
        evt.EvtNext(handle, size, events, Integer.MAX_VALUE, 0, returned)
      }

      if (index > returned.length - 1 && evt.EvtNext(handle, size, events, Integer.MAX_VALUE, 0, returned)) {
        index = 0
      }
      if (index < returned.length - 1) {
        eventContent(events(index)) match {
          case Some(value) =>
            ready()
            item = Event(None, new String(value))
            index += 1
          case _ =>
            finished()
        }
        evt.EvtClose(events(index))

      }


    }

    @throws(classOf[IOException])
    override def close() {
      super.close()
      evt.EvtClose(handle)
    }

    override def next(): Event = {
      init()
      item
    }
  }

}
