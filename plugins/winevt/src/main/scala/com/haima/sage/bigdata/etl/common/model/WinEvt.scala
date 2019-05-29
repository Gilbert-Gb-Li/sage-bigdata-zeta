package com.haima.sage.bigdata.etl.common.model

import com.sun.jna.Native

import com.sun.jna.platform.win32.WTypes
import com.sun.jna.platform.win32.WinDef
import com.sun.jna.platform.win32.WinDef.DWORD
import com.sun.jna.platform.win32.WinNT
import com.sun.jna.win32.StdCallLibrary
import com.sun.jna.win32.W32APIOptions

/**
 * Author:zhhuiyan
 *
 * DateTime:2014/8/6 17:32.
 */
object WinEvt {

  class EVT_HANDLE extends WinNT.HANDLE {
  }

  class EVT_HANDLEByReference extends WinNT.HANDLEByReference {
  }

  val INSTANCE: WinEvt = Native.loadLibrary("wevtapi", classOf[WinEvt], W32APIOptions.UNICODE_OPTIONS).asInstanceOf[WinEvt]
  val EvtRenderEventValues: Int = 0
  val EvtRenderEventXml: Int = 1
  val EvtRenderBookmark: Int = 2
  val EvtQueryChannelPath: Int = 0x1
  val EvtQueryFilePath: Int = 0x2
  val EvtQueryForwardDirection: Int = 0x100
  val EvtQueryReverseDirection: Int = 0x200
  val EvtQueryTolerateQueryErrors: Int = 0x1000
}

trait WinEvt extends StdCallLibrary {
  def EvtArchiveExportedLog(session: WinNT.HANDLE, locate: WinDef.LCID, flag: WinDef.DWORD): WinDef.BOOL

  def EvtCancel(session: WinNT.HANDLE): WinDef.BOOL

  def EvtClearLog(session: WinNT.HANDLE, path: String, filePath: String, flag: WinDef.DWORD): WinDef.BOOL

  def EvtClose(session: WinNT.HANDLE): WinDef.BOOL

  def EvtCreateBookmark(bookmark: String): WinNT.HANDLE

  def EvtCreateRenderContext(number: WinDef.DWORD, values: Array[String], flag: WinDef.DWORD): WinNT.HANDLE

  def EvtExportLog(session: WinNT.HANDLE, path: String, logtoFile: String, flag: WinDef.DWORD): WinDef.BOOL

  def EvtFormatMessage(PublisherMetadata: WinEvt.EVT_HANDLE, Event: WinEvt.EVT_HANDLE, MessageId: WinDef.DWORD, ValueCount: WinDef.DWORD, Values: Array[String], Flags: WinDef.DWORD, BufferSize: WinDef.DWORD, Buffer: WTypes.LPWSTR): WinDef.DWORD

  def EvtQuery(Session: WinEvt.EVT_HANDLE, Path: String, Query: String, Flags: Int): WinEvt.EVT_HANDLE

  def EvtNext(ResultSet: WinEvt.EVT_HANDLE, EventArraySize: Int, EventArray: Array[WinEvt.EVT_HANDLE], Timeout: Int, Flags: Int, Returned: Array[WinDef.DWORD]): Boolean

  def EvtRender(Context: WinEvt.EVT_HANDLE, Fragment: WinEvt.EVT_HANDLE, Flags: Int, BufferSize: WinDef.DWORD, Buffer: Array[Byte], BufferUsed: WinDef.DWORDByReference, PropertyCount: WinDef.DWORDByReference): Boolean
}
