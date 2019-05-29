package com.haima.sage.bigdata.etl.reader

import java.io.File
import java.util.Date

import com.haima.sage.bigdata.etl.common.Constants.CONF
import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.SingleChannel
import com.haima.sage.bigdata.etl.es_5.tools.{ConstantsTest, ModelingConfigTest}
import com.haima.sage.bigdata.etl.plugin.es_5.reader.FlinkES5ModelingReader
import com.haima.sage.bigdata.etl.utils.Mapper
import org.apache.flink.api.java.io.TextOutputFormat
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.Path
import org.junit.{Before, Test}


/**
  * Created by evan on 17-9-28.
  */
class ModelingTest extends Mapper {


  @Test
  def es5ReaderTest: Unit = {
    val file = s"${ConstantsTest.OUTPUT_PATH}/${filename("es5ReaderTest")}.tmp"
    val env = getEnv()
    val ds = new FlinkES5ModelingReader(SingleChannel(ModelingConfigTest.es5Source)).getDataSet(env)
        ds.collect().foreach(println(_))
        
//            .output(localOutput(file))
//    ds
//    env.execute("es5ReaderTest")
//    assertOutput(file)
  }

  @Test
  def analyzerWithES5ReaderTest: Unit = {
    val file = s"${ConstantsTest.OUTPUT_PATH}/${filename("analyzerWithES5ReaderTest")}.tmp"
    val env = getEnv()
    val dataSet = new FlinkES5ModelingReader(SingleChannel(ModelingConfigTest.es5Source)).getDataSet(env).output(localOutput(file))
    //    new ModelingSQLAnalyzer(ModelingConfigTest.analyzerWithES5).handle(
    //      dataSet
    //    ).foreach(
    //      _.output(localOutput(file))
    //    )
    env.execute("analyzerWithES5ReaderTest")
    assertOutput(file)
  }


  var longTime: Long = 0L

  def filename(head: String): String = s"$head-$longTime"

  def localOutput(file: String) = new TextOutputFormat[RichMap](new Path(file))

  def assertOutput(file: String): Unit = {
    val output = new File(file)
    assert(output.exists() && output.isFile && output.length() > 0)
  }

  def getEnv(): ExecutionEnvironment = {
    val _env = ExecutionEnvironment.getExecutionEnvironment
    _env.setParallelism(CONF.getInt("flink.parallelism"))
    _env
  }

  @Before
  def before: Unit = {
    longTime = new Date().getTime
  }
}
