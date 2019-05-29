package com.haima.sage.bigdata.etl.reader

import java.util

import com.haima.sage.bigdata.analyzer.modeling.reader.FlinkModelingReader
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{RichMap, SingleChannel}
import com.haima.sage.bigdata.etl.input.JsonInputFormat
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.java.hadoop.mapred.wrapper.HadoopInputSplit
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}
import org.apache.flink.hadoopcompatibility.scala.HadoopInputs
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapred.FileInputFormat.addInputPath
import org.apache.hadoop.mapred.JobConf

import scala.reflect.ClassTag

/**
  * Modeling Reader for HDFS
  *
  * @param channel
  */
class FlinkHDFSModelingReader(override val channel: SingleChannel) extends FlinkModelingReader {
  private def createInputFormat(): InputFormat[(LongWritable, util.Map[String, Object]), HadoopInputSplit] = {
    val configuration = new JobConf()
    configuration.setBoolean("dfs.support.append", true)
    configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
    configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
    configuration.set("dfs.block.access.token.enable", "true")
    configuration.set("dfs.http.policy", "HTTP_ONLY")
    configuration.set("dfs.replication", "1")
    configuration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    configuration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    configuration.set("fs.hdfs.impl.disable.cache", "true")

    val hadoopInputFormat = HadoopInputs.createHadoopInput[LongWritable, util.Map[String, Object]](
      new JsonInputFormat(),
      classOf[LongWritable],
      classOf[util.Map[String, Object]],
      configuration)(createTypeInformation[(LongWritable, util.Map[String, Object])])
    addInputPath(hadoopInputFormat.getJobConf(), new Path(channel.dataSource.uri))
    hadoopInputFormat
  }

  override def getDataSet(evn: ExecutionEnvironment): DataSet[RichMap] = {
    import scala.collection.JavaConversions._
    evn.createInput[(LongWritable, util.Map[String, Object])](createInputFormat())(ClassTag(classOf[(LongWritable, util.Map[String, Object])]), createTypeInformation[(LongWritable, util.Map[String, Object])])
      .map(_._2.toMap)
  }
}