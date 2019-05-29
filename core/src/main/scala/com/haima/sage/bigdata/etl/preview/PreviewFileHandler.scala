package com.haima.sage.bigdata.etl.preview

import java.io.{BufferedInputStream, BufferedReader, InputStream, InputStreamReader}

import akka.util.Timeout
import com.haima.sage.bigdata.etl.codec.Codec
import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.common.exception.LogReaderInitException
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter.{AddFields, MapRule}
import com.haima.sage.bigdata.etl.monitor.file.JavaFileUtils
import com.haima.sage.bigdata.etl.reader.FileLogReader

/**
  * Created by zhhuiyan on 7/6/2015.
  */
class PreviewFileHandler[F] {

  @SuppressWarnings(Array("unchecked"))
  def handle(conf: FileSource,
             parser: Parser[MapRule],
             file: InputStream): (Option[FileLogReader[F]], Option[Parser[MapRule]]) = {
    var pnew: Parser[MapRule] = null
    try {

      import scala.concurrent.duration._
      implicit val timeout = Timeout(5 minutes)
      val position = ReadPosition(conf.uri, 0, 0)
      val reader: BufferedReader = conf.encoding match {
        case Some(encoding: String) =>
          new BufferedReader(new InputStreamReader(file, encoding), 100000)
        case _ =>
          val charset = JavaFileUtils.charset(new BufferedInputStream(file))
          val buf = new BufferedReader(new InputStreamReader(file, charset), 100000)
          buf
      }
      val lineSeparator: String = JavaFileUtils.lineSeparator(reader)
      if (conf.category.contains("iis")) {
        var header: Map[String, String] = Map()
        if (reader.markSupported)
          reader.mark(1024 * 1024 * 100)
        var i: Int = 0
        while (i < 4) {
          val line = reader.readLine
          if (line != null && line.startsWith("#")) {
            line.substring(1).split(":") match {
              case Array("Fields", first: String) =>
                pnew = Delimit(Some(" "), first.trim.split(" "), parser.filter)
              case Array(first: String, second: String) =>
                header += ((first, second))
              case _ =>
            }
          }
          else {
            if (reader.markSupported) {
              reader.reset()
            }
            /*reader match {
              case seekable: Seekable =>
                seekable.seek(0)
              case _ =>
            }*/
            i = 5
          }
          i += 1
        }
        pnew match {
          case null =>
          case delimit: Delimit =>
            delimit.filter match {
              case rules if rules != null && rules.nonEmpty =>
                pnew = Delimit(delimit.delimit, delimit.fields, rules ++ Array(AddFields(header)))
              case _ =>
                pnew = Delimit(delimit.delimit, delimit.fields, Array(AddFields(header)))
            }

        }
      }
      val logReader: FileLogReader[F] = {
        val clazz: Class[FileLogReader[F]] = Class.forName(Constants.READERS.getString(conf.contentType.get.name
        )).asInstanceOf[Class[FileLogReader[F]]]
        clazz.getConstructor(
          classOf[String],
          classOf[Option[Codec]],
          classOf[BufferedReader],
          classOf[ReadPosition],
          classOf[Int]).
          newInstance(conf.uri, conf.codec, reader, position, Integer.valueOf(lineSeparator.length))
      }
      //logger.debug(" starting  processor file:{} ... ", uri)
      //logger.debug(s"${logReader.path},${logReader.position}")
      (Some(logReader), Some(if (pnew == null) parser else pnew))
    } catch {
      case e: LogReaderInitException =>
        //logger.info(s"${e.getMessage}")
        (None, None)
      case e: Exception =>
        //logger.error("FILE HANDLER ERROR", e)
        (None, None)
    }

  }

}
