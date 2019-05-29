package com.haima.sage.bigdata.etl.driver

import java.net.URI

import com.haima.sage.bigdata.etl.common.model.ReadPosition
import com.haima.sage.bigdata.etl.monitor.file.HDFSFileWrapper
import com.haima.sage.bigdata.etl.reader.TXTFileLogReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.junit.Test

/**
  * Created by bbtru on 2017/10/10.
  */
class HDFSFileWrapperTest {

  val path = "/tao"
  val uri =  "/data/takeout/snapshot/table_takeout_active_shop/dt=2018-08-21"
//  val path = "/hbase"
//  val uri = "hdfs://10.10.106.69:9000"

  /**
    * 测试目录是否存在
    */
  @Test
  def testExists(): Unit = {
    val fs = FileSystem.get(new URI(uri), new Configuration)
    val hdfsFileWrapper = new HDFSFileWrapper(fs, new Path(path), "", Option("UTF-8"))
    assert(hdfsFileWrapper.exists() == true, s"指定目录 [$path] 不存在.")
  }

  @Test
  def testHAExists(): Unit = {
    val configuration = new Configuration
    //    configuration.setBoolean("dfs.support.append", true)
    //    configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
    //    configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
    //    configuration.set("dfs.block.access.token.enable", "true")
    //    configuration.set("dfs.http.policy", "HTTP_ONLY")
    //    configuration.set("dfs.replication", "1")
    //    configuration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    //    configuration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    //    configuration.set("fs.hdfs.impl.disable.cache", "true")

    configuration.set("fs.defaultFS", s"hdfs://HaimaDev")
    configuration.set("dfs.nameservices","HaimaDev")
    configuration.set("dfs.ha.namenodes.HaimaDev", "nn1,nn2")
    configuration.set("dfs.client.failover.proxy.provider.HaimaDev", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    configuration.set("dfs.namenode.rpc-address.HaimaDev.nn1", "bigdata-dev01:8020")
    configuration.set("dfs.namenode.rpc-address.HaimaDev.nn1", "bigdata-dev02:8020")
    val fs = FileSystem.get(new URI(uri), configuration)
    val hdfsFileWrapper = new HDFSFileWrapper(fs, new Path(uri), "", Option("UTF-8"))
    assert(hdfsFileWrapper.exists() == true, s"指定目录 [$path] 不存在.")
  }

  /**
    * 获取指定目录文件列表
    */
  @Test
  def testListFiles(): Unit = {
    val fs = FileSystem.get(new URI(uri), new Configuration)
    val hdfsFileWrapper = HDFSFileWrapper(fs, new Path(path), "", Option("UTF-8"))
    val list = hdfsFileWrapper.listFiles()
    while(list.hasNext) {
      val item = list.next()
      println("名称: "+item.name+" 绝对路径: "+item.absolutePath+" Meta: "+item.metadata)
    }
  }

  /**
    * 测试读取文件内容
    */
  @Test
  def testInputStream(): Unit = {
    val fs = FileSystem.get(new URI(uri), new Configuration)
    val hdfsFileWrapper = new HDFSFileWrapper(fs, new Path(path), "", Option("UTF-8"))


  }

  @Test
  def hdfsWrapper(): Unit ={
    val fs = FileSystem.get(new URI(uri), new Configuration)
    val wrapper = new HDFSFileWrapper(fs, new Path(path+"/jdjtest"), "", Option("UTF-8"))
    val logReader = new TXTFileLogReader("/tao/jdjtest",None,None,wrapper,ReadPosition(path,0,0))
 //   println("size:"+logReader.stream.size)
   // logReader.stream.zipWithIndex.foreach(println)

//    val ddd=logReader.iterator
//    ddd.isEmpty
//    var i=0
//    while (ddd.hasNext){
//      i+=1
//
//      if(i==100)
//        return
//    }
//    println("size:"+logReader.iterator.size)
    logReader.iterator.zipWithIndex.foreach{
      case (x,index)=>{
        println(index,x)
      }
    }

//    logReader.stream.foreach(x=>{
//      println(x)
//    })
//    val reader = wrapper.stream
//    var str = reader.readLine()
//    var i=1
//    while(str!=""){
//      i = i+1
//      println(str)
//      str = reader.readLine()
//    }
//    println("stream:"+i)
  }


}
