package com.haima.sage.bigdata.etl.server.knowledge

import java.nio.file.Paths

import com.haima.sage.bigdata.etl.codec.Codec
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.knowledge.KnowledgeLoader
import com.haima.sage.bigdata.etl.monitor.file.LocalFileWrapper
import com.haima.sage.bigdata.etl.reader.FileLogReader
import com.haima.sage.bigdata.etl.utils.Logger

class FileKnowledgeLoader[T](override val source: FileSource) extends KnowledgeLoader[FileSource, T] with Logger {


  lazy val reader: FileLogReader[T] = {
    val path1 = try {
      Paths.get(source.path)
    } catch {
      case e: Exception =>
        logger.error(s"check path error :$e")
        null
    }
    val file = LocalFileWrapper(path1, source.encoding)
    val clazz: Class[FileLogReader[T]] = Class.forName(Constants.READERS.getString(
      source.contentType match {
        case Some(value) =>
          value.name
        case None =>
          "txt"
      }
    )).asInstanceOf[Class[FileLogReader[T]]]

    clazz.getConstructor(
      classOf[String],
      classOf[Option[Codec]],
      classOf[Option[FileType]],
      classOf[FileWrapper[_]],
      classOf[ReadPosition]).
      newInstance(file.path.toString, source.codec,source.contentType, file, ReadPosition(source.path, 0, 0))
  }


  /** 加载数据接口 **/
  override def load(): Iterable[T] = {
    reader
  }

  /**
    * 分页数据大小
    *
    * @return (数据,是否还有)
    */
  override def byPage(size: Int): (Iterable[T], Boolean) = {
   val data= reader.take(10000l, size)
    if(data._1==Nil)
      (data._1,data._2)
    else{
      (data._1,!data._2)
    }
  }
}
