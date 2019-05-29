package com.haima.sage.bigdata.etl.driver.usable


import java.io.{File, FileReader}

import com.haima.sage.bigdata.etl.common.model.{DataSource, Usability, UsabilityChecker, Writer}
import com.haima.sage.bigdata.etl.driver.{FileDriver, FileMate}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success}

/**
  * Created by evan on 17-8-11.
  */
case class FileUsabilityChecker(mate: FileMate) extends UsabilityChecker {

  val driver = FileDriver(mate)

  lazy val logger = LoggerFactory.getLogger(FileUsabilityChecker.getClass)

  override protected def msg: String = mate.path + " has error:"

  override def check: Usability = {

    driver.driver() match {
      case Success(file) =>
        try {
          /* 判断文件是否存在，不存在则创建文件 */

          mate match {
            case m:DataSource =>{
              if(file.isFile){
                if(!canRead(file)){
                  throw new Exception("Read Permission Denied")
                }
              }else if(file.isDirectory){
                val files = file.listFiles()
                if(files!=null && files.length>0){
                  var bool = true
                  val length = files.length
                  var i=0
                  var f:File = null
                  while(bool){
                    if(i>=length){
                      bool = false
                    }else{
                      if(files(i).isFile){
                        f = files(i)
                        bool = false
                      }
                    }
                    i+=1
                  }
                  if(!canRead(f)){
                    throw new Exception("Read Permission Denied")
                  }
                }
              }
            }
            case m:Writer =>{
              var bool:Boolean = false
              if (!file.exists) {
                /* 判断文件上级目录是否存在，不存在则创建目录 */
                if (!file.getParentFile.exists()) {
                  file.getParentFile.mkdirs()
                }
                file.createNewFile()
                bool = true
              }
              /* 判断file是否为文件，并且是否有写入权限，否则报出一场信息并停止采集器 */
              if (!file.isFile) {
                throw new Exception("Not File")
              }
              if(!file.canWrite){
                throw new Exception("Write Permission Denied")
              }
              if(bool)
                file.delete()
            }
          }
          Usability()
        } catch {
          case e: Exception => {
            logger.error("FileUsabilityChecker Error", e)
            Usability(usable = false, cause = s"$msg ${getErrorMsg(e)}")
          }
        }
      case Failure(e) =>
        logger.error("FileUsabilityChecker Error", e)
        Usability(usable = false, cause = s"$msg ${getErrorMsg(e)}")
    }
  }

  def canRead(file: File): Boolean ={
    if(file == null)
      return true
    var fd :FileReader= null
    try {
      if(!file.exists())
        return true
      fd = new FileReader(file)
      fd.read()
       true
    } catch  {
      case _: Throwable =>
         false
    } finally {
      if(fd!=null)
        fd.close()
    }
  }

  /**
    * 暂时没有用
    * @param file
    * @return
    */
  def canWrite(file: File): Boolean ={
    var fw:java.io.FileWriter = null;
    var result = false;
    try {
      if(!file.exists())
        return true
      fw = new java.io.FileWriter(file, true);
      fw.write("");
      fw.flush();
      result = true;
      return result;
    } catch  {
      case _: Throwable =>
        return false;
    } finally {
      if(fw !=null)
          fw.close()
    }
  }
  def getErrorMsg(e: Throwable) = if (e.getMessage != null) {
    e.getMessage
  } else {
    "Illegal path"
  }
}
