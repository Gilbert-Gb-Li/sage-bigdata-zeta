package com.haima.sage.bigdata.etl.metrics

import com.codahale.metrics.{Counter, Meter, Timer, Histogram}

/**
  * Created by evan on 17-10-11.
  *
  * Metric Meter信息包装类
  *
  * success：成功，读取操作、解析（分析）操作、写入操作包含
  * fail：失败，解析（分析）操作、写入操作包含
  * ignore：忽略，解析（分析）操作包含
  */
trait MetricsReport[T] {
  val success: Option[T]

  val fail: Option[T]

  val ignore: Option[T]

  val in: Option[T] = None
}

case class MeterReport(
                        override val success: Option[Meter],
                        override val fail: Option[Meter],
                        override val ignore: Option[Meter],
                        override val in: Option[Meter] = None
                      ) extends MetricsReport[Meter]

case class CounterReport(
                          override val success: Option[Counter],
                          override val fail: Option[Counter],
                          override val ignore: Option[Counter]
                        ) extends MetricsReport[Counter]

case class TimerReport(
                        override val success: Option[Timer],
                        override val fail: Option[Timer],
                        override val ignore: Option[Timer]
                      ) extends MetricsReport[Timer]

case class HistogramReport(
                            override val success: Option[Histogram],
                            override val fail: Option[Histogram],
                            override val ignore: Option[Histogram]
                          ) extends MetricsReport[Histogram]
