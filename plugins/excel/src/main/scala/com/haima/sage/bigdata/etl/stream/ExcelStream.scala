package com.haima.sage.bigdata.etl.stream

import java.text.SimpleDateFormat

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{ExcelFileType, FileWrapper, RichMap, Stream}
import org.apache.poi.hssf.usermodel.HSSFWorkbook
import org.apache.poi.ss.format.CellFormat
import org.apache.poi.ss.usermodel._
import org.apache.poi.xssf.usermodel.XSSFWorkbook

import scala.util.Try

/**
  * Created by jdj on 2017/7/4.
  */
object ExcelStream {

  def apply(wrapper: FileWrapper[_], contentType: Option[ExcelFileType] = None): ExcelStream = {
    new ExcelStream(wrapper: FileWrapper[_], contentType)
  }
}


class ExcelStream(wrapper: FileWrapper[_], contentType: Option[ExcelFileType] = None) extends Stream[RichMap](None) {

  val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  /* private lazy val file = Try {
    new RandomAccessFile(path, "r")
   }.toOption*/


  private lazy val inputStream = wrapper.inputStream

  private lazy val format = CellFormat.GENERAL_FORMAT
  private lazy val book: Option[java.util.Iterator[Sheet]] = {

    Try(if (wrapper.absolutePath.endsWith("xls")) {
      new HSSFWorkbook(inputStream).sheetIterator()
    } else {
      new XSSFWorkbook(inputStream).sheetIterator()
    }).toOption

  }

  private var sheet: java.util.Iterator[Row] = _


  private var header: Array[String] = _

  private var item: RichMap = _


  override def hasNext: Boolean = {
    state match {
      case State.ready =>
        true
      case State.done =>
        false
      case State.fail =>
        false
      case _ =>
        makeData()
        hasNext
    }
  }

  override def close(): Unit = {
    finished()
    try {
      wrapper.close()

    } catch {
      case e: Throwable =>
        logger.warn("close excel file exception {}", e.getMessage)
    }

    super.close()
  }

  override def next(): RichMap = {
    state = State.init
    item
  }

  val formatter = new DataFormatter()

  private def getValue(cell: Cell, defualt: Any = null): Any = {

    cell match {
      case null =>
        defualt
      case _ =>

                  cell.getCellTypeEnum match {
                    case CellType.BOOLEAN =>
                      cell.getBooleanCellValue
                    case CellType.NUMERIC =>
                      if (DateUtil.isCellDateFormatted(cell)) {
                        cell.getDateCellValue()
                      } else {

                        formatter.formatCellValue(cell)

                      }

                    case CellType.FORMULA =>
                      format.apply(cell).text
                    case _ =>
                      cell.getStringCellValue
                  }
    }

  }

  /*当数据没准备完整时,hasNext 会一直调用,直到数据准备完整*/
  private def makeData(): Unit = {
    try {
      book match {
        case Some(b) =>

          if (sheet == null) {
            if (b.hasNext) {
              b.next() match {
                case null =>
                case d =>
                  sheet = d.rowIterator()
              }
            } else {
              close()
            }

          }
          if (sheet != null) {
            if (!sheet.hasNext) {
              sheet = null
              header = null
            } else {
              val row = sheet.next()
              if (row != null) {
                if (null != contentType.get.header && !"".equals(contentType.get.header))
                  header = contentType.get.header.split(",")
                if (header == null) {
                  header = (0 to row.getLastCellNum).map(index => getValue(row.getCell(index), "cell" + index)).map(_.toString).toArray[String]
                } else {
                  if (header.length < row.getLastCellNum)

                    item = header.zip((0 to header.length).map(index => getValue(row.getCell(index))).toArray[Any]).filter(_._2 != null).toMap
                  else {
                    item = header.zip((0 to row.getLastCellNum).map(index => getValue(row.getCell(index))).toArray[Any]).filter(_._2 != null).toMap
                  }

                  ready()
                }
              }
            }
          }

        case _ =>
          close()
      }

    } catch {
      case e: Exception =>
        logger.error("process excel file exception {}", e.getMessage)
        close()
        state = State.fail
    }

  }
}
