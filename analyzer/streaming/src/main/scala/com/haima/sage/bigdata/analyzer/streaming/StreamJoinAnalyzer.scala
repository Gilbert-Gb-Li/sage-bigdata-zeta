package com.haima.sage.bigdata.analyzer.streaming

import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.DataStream

/**
  * Created by evan on 17-8-29.
  */
class StreamJoinAnalyzer(override val conf: Join) extends JoinDataAnalyzer[DataStream[RichMap]] with Logger {

  import com.haima.sage.bigdata.analyzer.streaming.utils._

  override def join(first: DataStream[RichMap], second: DataStream[RichMap]): DataStream[RichMap] = {


    getStream(first, conf.first).join(getStream(second, conf.second))
      .where(_.getOrElse(conf.first.joinField, ""))
      .equalTo(_.getOrElse(conf.second.joinField, ""))
      .window(conf.window.assigner())
      .apply(exec)
  }

  def getStream(data: DataStream[RichMap], config: JoinConfig): DataStream[RichMap] = {
    data.executionEnvironment.setParallelism(CONF.getInt("flink.parallelism"))

    data.assign(config.timeCharacteristic)

  }

  def engine(): String = "streaming"
}
