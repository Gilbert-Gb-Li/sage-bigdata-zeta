package com.haima.sage.bigdata.analyzer.streaming

import com.haima.sage.bigdata.etl.common.model._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

package object utils {

  implicit class AssignerWindows(window: Window) {
    implicit def assigner(): WindowAssigner[AnyRef, TimeWindow] = {
      window match {
        case TumblingWindow(w, ProcessingTime()) =>
          TumblingProcessingTimeWindows.of(Time.milliseconds(w))
        case TumblingWindow(w, EventTime(_, _)) =>
          TumblingEventTimeWindows.of(Time.milliseconds(w))
        case SlidingWindow(sliding, w, ProcessingTime()) =>
          SlidingProcessingTimeWindows.of(Time.milliseconds(w), Time.milliseconds(sliding))
        case SlidingWindow(sliding, w, EventTime(_, _)) =>
          SlidingEventTimeWindows.of(Time.milliseconds(w), Time.milliseconds(sliding))

        case SessionWindow(gap, ProcessingTime()) =>
          ProcessingTimeSessionWindows.withGap(Time.milliseconds(gap))
        case SessionWindow(gap, EventTime(_, _)) =>
          EventTimeSessionWindows.withGap(Time.milliseconds(gap))

      }


    }
  }

  implicit class WithAssigner(data: DataStream[RichMap]) extends Serializable {
    def assign(timeCharacteristic: TimeType = ProcessingTime()): DataStream[RichMap] = {
      timeCharacteristic match {
        case ProcessingTime() =>
          data.executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.valueOf("ProcessingTime"))
          data
        case IngestionTime() =>
          data.executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.valueOf("IngestionTime"))
          data
        case EventTime(field, maxOutOfOrderness) =>
          data.executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.valueOf("EventTime"))
          data.assignTimestampsAndWatermarks(Assigner(field, maxOutOfOrderness))
      }

    }
  }

}
