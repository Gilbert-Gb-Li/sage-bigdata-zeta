package com.haima.sage.bigdata.etl.performance.service

import java.text.SimpleDateFormat
import java.util.Date

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object GuajiCarPriceInfoUtils {
  val rd = new util.Random
  private val field = "data"
  private val textService = new TextMatchService(field, Seq(
    TextSingleConfig("price", TextFindConfig("全价购买(\\d+\\.?\\d*万).*")),
    TextSingleConfig("month_payment", TextFindConfig("月供：(\\d+)元.*")),
    TextUpConfig("down_payment",
      TextFindConfig("月供：(\\d+)元.*", -1),
      TextFindConfig("(^\\d+\\.?\\d*万)元")
    ),
    TextUpConfig("car_name",
      TextFindConfig(".*金融产品大升级.*"),
      TextFindConfig("\\d{4}款", firstGroup = false, matchType = false)
    )
  ),
    attrs = Seq(XmlToTextConfig("content-desc")),
    cleans = Seq("关注\n我要优惠\n电话客服\n在线咨询\n", "我要优惠\n", "车价分析\n")
  )

  private val regexService = new RegexService(
    new RegexConfig("schema",
      ".*;link_uid=(\\d+)/.*",
      Map[Int, String](1 -> "car_uid")),
    new RegexConfig("schema",
      ".*S.puid=(.+);.*",
      Map[Int, String](1 -> "car_uid"))
  )
  private val numberService = new NumberService(
    NumberItemConfig("down_payment", "wan"),
    NumberItemConfig("price", "wan"))
  private val filedTypeService = FiledTypeService(
    FiledTypeConfig("date_time", "date", "yyyy-MM-dd"),
    FiledTypeConfig("month_payment", "int"),
    FiledTypeConfig("down_payment", "int"),
    FiledTypeConfig("price", "int")
  )
  private val mergeService = MergeService(MergeConfig(
    "@es_id", Seq("meta_app_name", "meta_table_name", "car_uid")
  ))

  private val baseConditionService = BaseConditionService(
    BaseConditionConfig(event => {
      val value: String = event.getOrElse("car_uid", "").toString
      if (value.nonEmpty) {
        event += ("car_uid" -> value.replaceAll("/.*",""))
      } else {
        event += ("car_uid" -> s"${rd.nextInt(10000)}unknown")
      }
    }),
    BaseConditionConfig(event => {
      var data_generate_time = event.getOrElse("timestamp",
        new Date().getTime.toString)
      val hdfsTime = new SimpleDateFormat("yyyy-MM-dd/HH")
        .format(new Date(data_generate_time.toString.toLong))
      val hdfsPath = s"/data/guaji/origin/car_price/$hdfsTime/data"
      event += ("@hdfs_path" -> hdfsPath)
    }))

  def process(event: RichMap): RichMap = Try {
    var dst: mutable.Map[String, Any] = new mutable.HashMap[String, Any]()
    dst ++= event
    dst += ("meta_app_name" -> "guaji")
    dst += ("meta_table_name" -> "car_price")
    dst = textService.process(dst)
    dst = regexService.process(dst)
    dst = numberService.process(dst)
    dst = filedTypeService.process(dst)
    baseConditionService.process(dst)
    dst = mergeService.process(dst)
    RichMap(dst.toMap)
  } match {
    case Success(value) =>
      value
    case Failure(exception) =>
      exception.printStackTrace()
      event
  }

}
