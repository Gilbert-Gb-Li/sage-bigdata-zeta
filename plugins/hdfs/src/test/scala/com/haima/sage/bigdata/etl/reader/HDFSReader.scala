package com.haima.sage.bigdata.etl.reader

import java.io.File
import java.util.Date

import com.haima.sage.bigdata.analyzer.modeling.reader.FlinkModelingReader
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.SingleChannel
import com.haima.sage.bigdata.etl.hdfs.tools.{ConstantsTest, ModelingConfigTest}
import org.apache.flink.api.java.io.TextOutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.Path
import org.junit.{Before, Test}

class HDFSReader {

  Constants.init("sage-hdfs.conf")
  def localOutput(file: String) = new TextOutputFormat[RichMap](new Path(file))

  def assertOutput(file: String): Unit = {
    val output = new File(file)
    assert(output.exists() && output.isFile && output.length() > 0)
  }


  var longTime: Long = 0L

  @Before
  def before: Unit = {
    longTime = new Date().getTime
  }

  def filename(head: String): String = s"$head-$longTime"

  def getEnv(): ExecutionEnvironment = {
    val _env = ExecutionEnvironment.createRemoteEnvironment("127.0.0.1", 6123, List("/opt/IdeaProjects/project/sage/sage-hdfs/target/sage-hdfs-3.0.0.5.jar"): _*)
    _env.setParallelism(CONF.getInt("flink.parallelism"))
    _env
  }

  @Test
  def hdfsReaderTest: Unit = {
    val file = s"${ConstantsTest.OUTPUT_PATH}/${filename("hdfsReaderTest")}.tmp"
    val env = getEnv()
    // reader(SingleChannel(ModelingConfigTest.hdfsSource, Some(ModelingConfigTest.parser))).getDataSet(env).output(localOutput(file))
    env.execute("hdfsReaderTest")
    assertOutput(file)
  }

  def reader(channel: SingleChannel): FlinkModelingReader = {
    val name: String = Constants.CONF.getString(s"app.modeling.reader.${channel.dataSource.name}")
    new FlinkHDFSModelingReader(channel)
    //
    //
    //    val clazz = Class.forName(name).asInstanceOf[Class[FlinkModelingReader]]
    //    clazz.getConstructor(source.getClass).newInstance(source)

  }

  @Test
  def analyzerWithHDFSReaderTest: Unit = {
    val file = s"${ConstantsTest.OUTPUT_PATH}/${filename("analyzerWithHDFSReaderTest")}.tmp"
    val env = getEnv()
    //val dataSet = reader(SingleChannel(ModelingConfigTest.hdfsSource, Some(ModelingConfigTest.parser))).getDataSet(env).output(localOutput(file))
    //    new ModelingSQLAnalyzer(ModelingConfigTest.analyzerWithHDFS).handle(
    //      dataSet
    //    ).foreach(
    //      _.output(localOutput(file))
    //    )
    env.execute("analyzerWithHDFSReaderTest")
    assertOutput(file)
  }
}
