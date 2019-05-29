package com.haima.sage.bigdata.etl.reader

import java.nio.file.Paths

import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model.{CSVFileType, ReadPosition}
import com.haima.sage.bigdata.etl.monitor.file.LocalFileWrapper
import org.junit.Test

import scala.io.Source

class CSVFileReaderTest {

  val path = Paths.get("/Users/zhhuiyan/Desktop/kunming.csv")

  @Test
  def reader(): Unit = {
    val reader = new CSVFileLogReader(path.toAbsolutePath.toString, contentType = Some(CSVFileType()),
      wrapper = LocalFileWrapper(path),
      position = ReadPosition(path.toString, 1, 88))

    reader.take(100, 1)
    // println(reader.position.position)

    assert(reader.toList.size == 98)


  }


  @Test
  def reader2(): Unit = {


    /*shop_id,question_type,entity_id,ocr_license_no,take_out_type,shop_name,shop_expire_date,license_expire_date,is_has_business_license,shop_license_no,org_code,geo_hash,comment_score,negative_comment_rate,comment_total_num,negative_comment_num,has_license,province,city,shop_logo,open_time,address,phone,shop_cert_photo1,shop_cert_photo2,d.orgname,question_type_0,question_type_6,question_type_7,question_type_8,question_type_9,question_type_10,question_type_11,question_type_12,question_type_13,question_type_14*/

    /* 声明file对象 */
    val file2 = new java.io.File("/Users/zhhuiyan/Downloads/kunming_all_shop-3.csv")
    /* 判断文件是否存在，并捕获异常信息，出现异常则停止采集器 */
    try {
      /* 判断文件是否存在，不存在则创建文件 */
      if (!file2.exists) {
        /* 判断文件上级目录是否存在，不存在则创建目录 */
        if (!file2.getParentFile.exists()) {
          file2.getParentFile.mkdirs()
        }
        file2.createNewFile()
      }
    } catch {
      case e: Exception => {
      }
    }

    def convert(ty: String): String = {
      ty match {
        case "1" =>
          "美团外卖"
        case "2" =>
          "饿了么"
        case "3" =>
          "百度外卖"
        case t =>
          t
      }
    }

    val w = file2.bufferedWriter(true)
    w.append("shop_id\tocr_license_no\ttake_out_type\tshop_name\tshop_expire_date\tlicense_expire_date\tis_has_business_license\tshop_license_no\torg_code\thas_license\tprovince\tcity\taddress\tphone\tshop_cert_photo1\tshop_cert_photo2\torgname\tlicno\tlicexpiry\tquestion_type_0\tquestion_type_6\tquestion_type_7\tquestion_type_8\tquestion_type_9\tquestion_type_10\tquestion_type_11\tquestion_type_12\tquestion_type_13\tquestion_type_14")
    w.newLine()
    Source.fromFile("/Users/zhhuiyan/Downloads/kunming_all_shop.csv").getLines().map(_.split("\t")).toList.groupBy(t => t(0)).map(data => {
      data._2.foldLeft(Array[String]())((f, s) => {

        if (f.isEmpty) {
          s.zipWithIndex.map(t => {
            if (t._2 == 3) {
              convert(t._1)
            } else if (t._2 >= 20) {
              if (t._1 == "NULL") {
                "0"
              } else {
                "1"
              }
            } else {
              t._1
            }
          })
        } else {
          f.zip(s).zipWithIndex.map(t => {

            if (t._2 == 3) {
              convert(t._1._1)
            } else if (t._2 >= 20) {
              if (t._1._1 == "0" && t._1._2 == "NULL") {
                "0"
              } else {
                "1"
              }
            } else {
              t._1._1
            }
          })
        }


      })
    }).map(_.mkString("\t")).foreach(line => {
      w.append(line)
      w.newLine()
    })
    w.flush()
    w.close()


  }
}
