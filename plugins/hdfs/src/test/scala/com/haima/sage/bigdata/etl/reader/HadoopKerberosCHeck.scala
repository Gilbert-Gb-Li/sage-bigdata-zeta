package com.haima.sage.bigdata.etl.reader

import java.io.File
import java.net.{URL, URI}
import java.nio.file.Paths
import javax.security.auth.login.LoginContext

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.security.{UserGroupInformation, SecurityUtil}
import org.junit.Test
import javax.security.auth.callback.{Callback, CallbackHandler}

/**
 * Created by zhhuiyan on 15/6/17.
 */
class HadoopKerberosCHeck {
  @Test
  def login(): Unit ={
    try{
     // System.setProperty("javax.security.auth.useSubjectCredsOnly", "false")
       var path: String =  this.getClass.getResource("/").getPath
      if (path.endsWith("test-classes/")) {
        path = path.replace("test-classes", "classes")
      }
     /*  println(path+"jaas.conf")
      System.setProperty("java.security.auth.login.config", path+"jaas.conf")

      val cbh:CallbackHandler =new CallbackHandler {
        override def handle(callbacks: Array[Callback]): Unit =
          callbacks.foreach{
            cb=>
              println( cb.toString)
          }
      }
      val context = new LoginContext("Client",cbh)

      context.login()*/

            val configuration = new Configuration
         //   configuration.set("fs.default.name","hdfs://172.16.219.173:9000")
            val PRINCIPAL = "dfs.namenode.kerberos.principal"
            val KEYTAB = "dfs.namenode.keytab.file"
       val KRB = "java.security.krb5.conf"
       System.setProperty("java.security.krb5.conf", configuration.get(KRB))
            // 设置keytab密钥文件
           // configuration.set(KEYTAB, "kerberos.keytab")
           // configuration.set("sun.security.krb5.conf", "krb5.conf")
     //  configuration.set("java.security.auth.login.config",  path+"jaas.conf")
            /*
                configuration.set(PRINCIPAL, "root/kunlun")
             *  <name>dfs.block.access.token.enable</name>
             *  <value>true</value>
             *  <name>dfs.datanode.kerberos.principal</name>
             *  <value>root/kunlun@HADOOP.COM</value>
             *
            // 设置kerberos配置文件路径
            System.setProperty("java.security.krb5.conf", "krb5.conf") */
       /* UserGroupInformation.setConfiguration(configuration)
        UserGroupInformation.loginUserFromKeytab("root/kunlun@HADOOP.COM", "kerberos.keytab")*/
      // 进行登录认证
      SecurityUtil.login(configuration, KEYTAB, PRINCIPAL)
      val uri = new URI("hdfs://kunlun:9000")
      val fs=    FileSystem.get(uri, configuration)
     println(fs.isDirectory(new Path("/log/iis")))
    }catch {
      case e:Exception=>
        e.printStackTrace()
    }

  }
}
