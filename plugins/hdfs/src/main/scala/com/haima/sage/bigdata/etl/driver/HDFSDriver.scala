package com.haima.sage.bigdata.etl.driver

import java.io.File
import java.net.URI

import com.haima.sage.bigdata.etl.common.model.AuthType
import com.haima.sage.bigdata.etl.utils.Logger
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.security.SecurityUtil

import scala.util.Try

/**
  * Created by zhhuiyan on 2016/11/22.
  */
case class HDFSDriver(mate: HDFSMate) extends Driver[DistributedFileSystem] with Logger {

  val loader: ClassLoader = classOf[HDFSDriver].getClassLoader
  val hosts: Array[String] = mate.host.split(",")
  val nameNodes: Array[String] = mate.nameNodes.split(",")
  val size = if(nameNodes.length != hosts.length) {
    logger.warn(s"namenodes size ${nameNodes.length} not equal rpc-address size ${hosts.length}")
    if (nameNodes.length < hosts.length) nameNodes.length else hosts.length
  }else{nameNodes.length}
  val configuration = new Configuration
  mate.authentication match {
    case Some(AuthType.KERBEROS) =>
      logger.info(s" authentication for kerberos")
      val PRINCIPAL = "dfs.namenode.kerberos.principal"
      val KEYTAB = "dfs.namenode.keytab.file"
      val KRB = "java.security.krb5.conf"
      System.setProperty("java.security.krb5.conf", configuration.get(KRB))
      SecurityUtil.login(configuration, KEYTAB, PRINCIPAL)
      logger.info(s" authentication for kerberos finished")
    case Some(authentication) =>
      logger.warn(s"unSupport authentication for $authentication")
    case None =>
  }
  var path = loader.getClass.getResource("/").getPath
  path = s"$path${
    if (path.endsWith("classes/") || path.endsWith("conf/")) {
      ""
    } else {
      "conf/"
    }
  }/hdfs-site.xml"
  val file = new File(path)

  configuration.setBoolean("dfs.support.append", true)
  configuration.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
  configuration.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")
  configuration.set("dfs.block.access.token.enable", "true")
  configuration.set("dfs.http.policy", "HTTP_ONLY")
  configuration.set("dfs.replication", "1")
  configuration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  configuration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
  configuration.set("fs.hdfs.impl.disable.cache", "true")

  configuration.set("fs.defaultFS", s"hdfs://${mate.nameService}")
  configuration.set("dfs.nameservices", mate.nameService)
  configuration.set(s"dfs.ha.namenodes.${mate.nameService}", mate.nameNodes)
  configuration.set(s"dfs.client.failover.proxy.provider.${mate.nameService}", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
  for (i <- 0 until size) {
    configuration.set(s"dfs.namenode.rpc-address.${mate.nameService}.${nameNodes(i)}", hosts(i))
  }
  override def driver(): Try[DistributedFileSystem] = Try (FileSystem.get(new URI(s"hdfs://${mate.nameService}"), configuration).asInstanceOf[DistributedFileSystem])
}

/*
*
*single FileSystem one config
* */
//object CachedFileSystem {
//  private def atomic = new AtomicInteger(0)
//
//  private val cache: mutable.Map[String, (FileSystem, AtomicInteger)] = new mutable.LinkedHashMap[String, (FileSystem, AtomicInteger)]
//  private val lock = new ReentrantLock()
//
//
//  def apply(uri: URI, configuration: Configuration): FileSystem = {
//    lock.lock()
//    val key = uri.toString
//
//    val system = cache.get(key) match {
//      case Some((sys, _)) =>
//        cache(key)._2.incrementAndGet()
//        sys
//      case _ =>
//        val sys = FileSystem.get(uri, configuration)
//        cache.put(key, (sys, atomic))
//        sys
//    }
//
//
//    lock.unlock()
//    system
//  }
//
//  def close(uri: URI): Unit = {
//    lock.lock()
//    val key = uri.toString
//    cache.get(key) match {
//      case Some(_) =>
//        cache(key)._2.decrementAndGet()
//        if (cache(key)._2.intValue() <= 0) {
//          cache.remove(key).get._1.close()
//        }
//      case _ =>
//    }
//
//    lock.unlock()
//  }
//
//  /*class CachedFileSystem(uri: URI, system: FileSystem) extends FileSystem {
//    override def getFileStatus(f: Path): FileStatus = system.getFileStatus(f)
//
//    override def mkdirs(f: Path, permission: FsPermission): Boolean = system.mkdirs(f, permission)
//
//    override def rename(src: Path, dst: Path): Boolean = system.rename(src, dst)
//
//    override def open(f: Path, bufferSize: Int): FSDataInputStream = system.open(f, bufferSize)
//
//    override def listStatus(f: Path): Array[FileStatus] = system.listStatus(f)
//
//    override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int, replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = {
//      system.create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int, replication: Short, blockSize: Long, progress: Progressable)
//    }
//
//
//    override def getWorkingDirectory: Path = system.getWorkingDirectory
//
//    override def setWorkingDirectory(new_dir: Path): Unit = system.setWorkingDirectory(new_dir)
//
//    override def getUri: URI = system.getUri
//
//    override def delete(f: Path, recursive: Boolean): Boolean = system.delete(f, recursive)
//
//    override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = system.append(f, bufferSize, progress)
//
//    override def close(): Unit = CachedFileSystem.close(uri)
//
//    def getSystem():FileSystem = system
//  }*/
//
//}


