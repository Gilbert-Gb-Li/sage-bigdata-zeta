package com.haima.sage.bigdata.etl.lexer.filter

import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.Gzip
import com.haima.sage.bigdata.etl.filter.RuleProcessor
import org.slf4j.LoggerFactory

class GzipProcessor(override val filter: Gzip) extends RuleProcessor[RichMap, RichMap, Gzip] {
  private lazy val logger = LoggerFactory.getLogger(classOf[GzipProcessor])

  def getBytes(value: Any): Array[Byte] = {
    value match {
      case t: Array[Byte] =>
        t
      //Base64.getDecoder.decode(t)
      case t: String =>
        t.getBytes
      //Base64.getDecoder.decode(t)
      case t =>
        logger.warn("unsupport data type for gzip")
        t.toString.getBytes
      // Base64.getDecoder.decode(t.toString)
    }


  }

  override def process(event: RichMap): RichMap = {

    RichMap(event.map {
      case (key, value) if filter.fields.contains(key) =>
        try {
          val out = new ByteArrayOutputStream()
          val gzip = new GZIPOutputStream(out)
          gzip.write(getBytes(value))
          gzip.close()
          (key, new String(out.toByteArray))
        } catch {
          case e: Exception =>
            logger.warn(s"gzip field:$key with value:$value error use original data to writer")
            (key, value)
        }
      case (key, value) =>
        (key, value)
    })

  }
}