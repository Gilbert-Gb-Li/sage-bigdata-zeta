package com.haima.sage.bigdata.etl.lexer.filter

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.util.zip.GZIPInputStream

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.model.filter.Gunzip
import com.haima.sage.bigdata.etl.filter.RuleProcessor
import org.slf4j.LoggerFactory


class GunzipProcessor(override val filter: Gunzip) extends RuleProcessor[RichMap, RichMap, Gunzip] {
  private lazy val logger = LoggerFactory.getLogger(classOf[GunzipProcessor])

  def getBytes(value: Any): Array[Byte] = {
    value match {
      case t: Array[Byte] =>
        t
      //Base64.getDecoder.decode(t)
      case t: String =>
        t.getBytes
      //Base64.getDecoder.decode(t)
      case t =>
        logger.warn("unsupport data type for gunzip")
        t.toString.getBytes
      // Base64.getDecoder.decode(t.toString)
    }


  }

  override def process(event: RichMap): RichMap = {

    RichMap(event.map {
      case (key, value) if filter.fields.contains(key) =>
        val bytes = getBytes(value)
        try {
          val out = new ByteArrayOutputStream


          val in = new ByteArrayInputStream(getBytes(value))
          val gunzip = new GZIPInputStream(in)
          val buffer = new Array[Byte](1024)
          var n = 0
          while ( {
            n = gunzip.read(buffer)
            n >= 0
          }) out.write(buffer, 0, n)
          gunzip.close()
          (key, new String(out.toByteArray))
        } catch {
          case e: Exception =>
            e.printStackTrace()
            logger.warn(s"gunzip field:$key with value:${bytes.mkString(",")} error use original data to writer")
            (key, value)
        }


      case (key, value) =>
        (key, value)
    })

  }
}