package com.haima.sage.bigdata.etl.driver

/**
  * Created by zhhuiyan on 15/9/14.
  */


import java.io._
import java.net.URI
import java.security.PrivilegedExceptionAction

import com.haima.sage.bigdata.etl.monitor.file.HDFSFileWrapper
import com.haima.sage.bigdata.etl.utils.{IOUtils, PathWildMatch}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, _}
import org.apache.hadoop.security.UserGroupInformation
import org.junit.Test

class HDFSTest {
  val target = "hdfs://172.16.219.130:9000/logs/"
  val pathName = "/iis/"
  val config = new Configuration()
  val uri = URI.create(target)
  val fs = FileSystem.get(uri, config)

  @Test
  def krb5(): Unit = {
    val conf = new Configuration()
    try {
      conf.set("fs.defaultFS", "hdfs://ibm-r1-node7.ibmbi-nextgen.com:8020")
      UserGroupInformation.setConfiguration(conf)
      val ugi = UserGroupInformation.createProxyUser("user", UserGroupInformation.getUGIFromTicketCache("/tmp/krb5cc_0", "real"))
      System.out.println("--------------status---:" + UserGroupInformation.isLoginKeytabBased)
      System.out.println("---------AFTER LOGIN-----:")
      ugi.doAs[Void](new PrivilegedExceptionAction[Void]() {
        @throws[Exception]
        override def run: Void = {
          val fs = FileSystem.get(conf)
          val path = new Path("hdfs://10.132.100.224:8020/tmp/root")
          val statusArray = fs.listStatus(path)
          System.out.println("------------------------------" + fs.listStatus(path))
          var count = 0
          val sdf = null
          for (status <- statusArray) {
            val blockSize = status.getBlockSize
            val permission = status.getPermission + ""
            val replication = status.getReplication
            val owner = status.getOwner
            val paths = status.getPath + ""
            val file = status.isFile
            val length = status.getLen
            val group = status.getGroup
            System.out.println("BlockSize   :" + blockSize)
            System.out.println("Group   :" + group)
            System.out.println("Length  :" + length)
            System.out.println("Owner   :" + owner)
            System.out.println("Replication :" + replication)
            System.out.println("File     :" + file)
            System.out.println("Permission  :" + permission)
            System.out.println("Path    :" + paths)
            count += 1
            System.out.println("-------------without auth-----count---------------" + count)
          }
          null
        }
      })
    } catch {
      case e: Exception =>
        System.out.println("--------EXCEPTION________________")
        e.printStackTrace()
    }
  }

  @Test
  def wildMatch(): Unit = {
    val uri = new URI("hdfs://yzh:9000")

    val fs = FileSystem.get(uri, new Configuration)
    val path = new Path("/logs/iis")

    val matcher = new PathWildMatch[HDFSFileWrapper]() {

      override def getFileWrapper(path: String): HDFSFileWrapper = {
        HDFSFileWrapper(fs, new Path(path), "", Option("UTF-8"))
      }
    }
    val (key, values) = matcher.parse("/logs/iis")
    println(s"root:$key")

    // matcher.files(LocalFileWrapper(Paths.get(key+"/")),values).foreach(file=>println(file.getAbsolutePath))
  }

  @Test
  def testRead() {

    val path = new Path(pathName)
    handlerPath(path)


  }

  @Test
  def auth() {
    try {
      val ugi: UserGroupInformation = UserGroupInformation.createRemoteUser("root")
      ugi.doAs[Void](new PrivilegedExceptionAction[Void]() {
        @throws(classOf[Exception])
        def run(): Void = {
          val conf: Configuration = new Configuration
          conf.set("fs.defaultFS", "hdfs://172.16.219.173:9000/")
          conf.set("hadoop.job.ugi", "hbase")
          val fs: FileSystem = FileSystem.get(conf)
          fs.createNewFile(new Path("/user/hbase/test"))
          val status: Array[FileStatus] = fs.listStatus(new Path("/user/hbase"))

          status.foreach(s => {
            System.out.println(s.getPath)
          })
          null
        }
      })
    }
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def handlerPath(path: Path) {
    if (fs.isDirectory(path)) {
      val iterator = fs.listFiles(path, true)
      while (iterator.hasNext) {
        val status = iterator.next()
        System.out.println(status.getPath)
        //   System.out.println(status.getSymlink());
        System.out.println(status.getOwner)
        handlerPath(status.getPath)

      }
    } else if (fs.isFile(path)) {
      handlerFile(path)
    }
  }

  def handlerFile(path: Path) {
    val in = fs.open(path)
    val bis = new BufferedReader(new InputStreamReader(in), 1024 * 1024)
    var line: String = ""
    try {
      while ( {
        line = bis.readLine();
        line
      } != null) System.out.println(line)
    } finally {
      IOUtils.close(in)
    }
  }

  @Test
  def modify() {

    val name = "iis/ex131210"
    val suf = ".log"
    val i = 1
    val fis = new FileInputStream(new File("F:/data/" + name + suf))
    //读取本地文件
    val os = fs.append(new Path(target + name + "-" + i + suf))

    val reader = new BufferedReader(new InputStreamReader(fis), 4096)
    val line = reader.readLine()
    os.write(line.getBytes)
    os.flush()
    os.close()
    reader.close()
    fis.close()


  }

  @Test
  def upload() {

    val name = "sniffer/"
    val suf = ".xls"
    val i = 10
    val fis = new FileInputStream(new File("data/sniffer/20141029235000.xls"))
    //读取本地文件
    val os = fs.create(new Path(target + name + "/20141029235000-" + i + suf))
    //copy
    IOUtils.copy(fis, os, 4096, true)
    System.out.println(" copy finished ")

  }

  @Test
  def uploadDir() {

    val name = "iis/"
    val suf = ".log"
    val i = 8
    val f = new File("data/iislog/SZ_test")
    if (f.isDirectory) {
      val files = f.listFiles()
      if (files != null) {
        (0 to i).foreach { i =>

          files.foreach { file =>
            val fis = new FileInputStream(file)
            //读取本地文件
            val os = fs.create(new Path(target + name + "/iis" + i + "/" + file.getName))
            //copy
            IOUtils.copy(fis, os, 4096, true);
          }
        }


      }


    }

    System.out.println(" copy finished ")

  }

  @Test
  def delete() {
    fs.delete(new Path("/logs/writer.txt"), true)
  }

}
