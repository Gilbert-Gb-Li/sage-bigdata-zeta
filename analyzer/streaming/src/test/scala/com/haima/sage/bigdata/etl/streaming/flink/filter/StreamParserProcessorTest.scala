package com.haima.sage.bigdata.etl.streaming.flink.filter

import java.util.Date
import java.util.concurrent.TimeUnit

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.common.model.{NothingParser, SQLAnalyzer}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.junit.Test

import scala.io.Source

class StreamParserProcessorTest extends Serializable {}
