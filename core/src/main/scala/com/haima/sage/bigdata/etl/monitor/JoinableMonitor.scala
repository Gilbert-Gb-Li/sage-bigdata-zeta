package com.haima.sage.bigdata.etl.monitor

import com.haima.sage.bigdata.etl.common.model._


/**
  * Created by zhhuiyan on 15/1/28.
  */


class JoinableMonitor() extends Monitor {
  override def run(): Unit = {
    context.parent ! (ProcessModel.MONITOR, Status.RUNNING)
  }

  override def close(): Unit = {

  }
}
