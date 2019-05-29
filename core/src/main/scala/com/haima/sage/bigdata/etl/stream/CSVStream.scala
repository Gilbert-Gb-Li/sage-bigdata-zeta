package com.haima.sage.bigdata.etl.stream

import com.github.tototoshi.csv.{CSVFormat, CSVParser, DefaultCSVFormat, MalformedCSVException}
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.RichMap

import com.haima.sage.bigdata.etl.common.model.{FileWrapper, ReaderLineReader, Stream}

/**
  * Created by zhhuiyan on 2014/11/5.
  */
object CSVStream {

  /*FileWrapper[_]*/
  implicit def file2Stream(wrapper: FileWrapper[_], headers: Option[Array[String]] = None): CSVStream = new CSVStream(wrapper.stream, headers)

  //implicit def reader2Stream(reader: BufferedReader, headers: Option[Array[String]] = None): CSVStream = new CSVStream(reader,headers)

  class CSVStream(lineReader: ReaderLineReader, headers: Option[Array[String]] = None) extends Stream[RichMap](None) {


    def skip(skip: Long): Long = {
      lineReader.skip(skip)
    }

    implicit val format: CSVFormat = new DefaultCSVFormat {
      override val treatEmptyLineAsNil = true
    }
    private val parser = new CSVParser(format)

    def readNext(): Option[(List[String], Long)] = {

      @scala.annotation.tailrec
      def parseNext(lineReader: ReaderLineReader, leftOver: Option[String] = None): Option[(List[String], Long)] = {

        val nextLine = lineReader.readLine()
        if (nextLine == null) {
          if (leftOver.isDefined) {
            throw new MalformedCSVException("Malformed Input!: " + leftOver)
          } else {
            None
          }
        } else {
          val line = leftOver.getOrElse("") + nextLine
          parser.parseLine(line) match {
            case None =>
              parseNext(lineReader, Some(line))
            case result =>
              result.map((_, line.length))
          }
        }
      }

      parseNext(lineReader)
    }

    private val header = headers match {
      case Some(_header) =>
        _header

      case None =>
        readNext() match {
          case None =>
            throw new MalformedCSVException("Malformed Input!: need header but file is not contains ")
          case Some((head, length)) =>
            logger.debug("set header" + head)

            head.toArray

        }

    }


    def this(wrapper: FileWrapper[_]) {
      this(wrapper.stream)
    }

    var item: Map[String, Any] = _


    final def hasNext: Boolean = {
      state match {
        case State.ready =>
          true
        case State.done =>
          false
        case State.fail =>
          false
        case _ =>
          readNext() match {
            case None =>
              finished()
            case Some((data, length)) =>
              item = header.zip(data).toMap + ("length" -> length)
              ready()
          }

          hasNext
      }
    }


    override def next(): RichMap = {
      init()
      item
    }

    override def close(): Unit = {
      lineReader.close()
      super.close()
    }
  }

}

