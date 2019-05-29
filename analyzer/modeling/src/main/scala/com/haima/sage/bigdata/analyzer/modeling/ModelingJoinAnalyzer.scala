package com.haima.sage.bigdata.analyzer.modeling

import com.haima.sage.bigdata.etl.common.model.{Join, JoinDataAnalyzer, RichMap}
import org.apache.flink.api.scala._

/**
  * Created by evan on 17-8-29.
  */
class ModelingJoinAnalyzer(val conf: Join) extends JoinDataAnalyzer[DataSet[RichMap]] {
  override def join(first: DataSet[RichMap], second: DataSet[RichMap]): DataSet[RichMap] = {




    first
      .join(second)
      .where(_.getOrElse(conf.first.joinField, "").toString)
      .equalTo(_.getOrElse(conf.second.joinField, "").toString)
      .apply(exec)
  }

  override def engine(): String = "modeling"
}
