package com.haima.sage.bigdata.etl.reader

import akka.actor.ActorSelection
import com.haima.sage.bigdata.etl.common.model.{Opt, ReadPosition}

/**
  * Created by zhhuiyan on 15/3/13.
  */
trait Position {
  def position: ReadPosition

  def callback(batch: ReadPosition)(implicit positionServer: ActorSelection): Unit = {
      positionServer ! (Opt.PUT, batch)
  }
}
