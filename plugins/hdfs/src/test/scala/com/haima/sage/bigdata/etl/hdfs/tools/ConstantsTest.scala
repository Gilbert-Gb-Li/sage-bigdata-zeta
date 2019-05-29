package com.haima.sage.bigdata.etl.hdfs.tools

import com.typesafe.config.{Config, ConfigFactory}

/**
  * Created by evan on 17-9-28.
  */
object ConstantsTest {
  final def CONF: Config = ConfigFactory.load("sage-modeling-test.conf")

  private val INPUT_PATH = CONF.getString("modeling.test.path.input")
  val OUTPUT_PATH = CONF.getString("modeling.test.path.output")

  val FLINK_HOST = CONF.getString("modeling.test.flink.host")
  val FLINK_PORT = CONF.getInt("modeling.test.flink.port")

  val ES_HOST = CONF.getString("modeling.test.es.host")
  val ES_PORT = CONF.getInt("modeling.test.es.port.client")
  val ES_INDEX = CONF.getString("modeling.test.es.index")
  val ES_TYPE = CONF.getString("modeling.test.es.type")

  val HDFS_HOST = CONF.getString("modeling.test.hdfs.host")
  val HDFS_PORT = CONF.getInt("modeling.test.hdfs.port")
  val HDFS_INPUT_PATH = CONF.getString("modeling.test.hdfs.path.input")

  val ANALYZER_INPUT_PATH = s"$INPUT_PATH/analyzer"

  val WRITER_PATH = CONF.getString("modeling.test.file.path.write")
}
