package com.haima.sage.bigdata.etl.performance.service

import java.text.SimpleDateFormat
import java.util.Date

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object RenrenCarPriceInfoUtils {
  private val hdfsFormat = new SimpleDateFormat("yyyy-MM-dd/HH")
  private val field = "data"
  private val textService = new TextMatchService(field, Seq(
    TextSingleConfig("price_tmp", TextFindConfig("首期款", 1)),
    TextDownConfig("month_payment",
      TextFindConfig("月供.*", 1),
      TextFindConfig("(\\d+)元")
    ),
    TextUpConfig("car_name",
      TextFindConfig(".*马上申请.*"),
      TextFindConfig("\\d{4}款", firstGroup = false, matchType = false)
    )
  ),
    attrs = Seq(XmlToTextConfig("text")),
    cleans = Seq("关注\n我要优惠\n电话客服\n在线咨询\n", "我要优惠\n", "车价分析\n")
  )

  private val regexService = new RegexService(
    new RegexConfig("schema",
      ".*;link_uid=(.*)/.*",
      Map[Int, String](1 -> "car_uid")),
    new RegexConfig("price_tmp",
      "(\\d+.?\\d*)\\s*万起.*全款\\s*(\\d+.?\\d*)\\s*万",
      Map[Int, String](1 -> "down_payment", 2 -> "price"),
      matchType= false)
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
  val rd = new util.Random
  private val conditionsService = ConditionsService(
    ConditionsConfig(Prediction(condition=true),
      Some(Calculations(List("car_uid"),
        list=> list.head.toString.split("/")(0)
      )), "car_uid", s"${rd.nextInt(10000)}unknown"),
    ConditionsConfig(
      Prediction(condition=true),
      Some(Calculations(List("timestamp", "meta_app_name", "meta_table_name"),
        list=>{
          val hdfsTime = if (list.head == null) {
            hdfsFormat.format(new Date())
          }else {
            hdfsFormat.format(new Date(list.head.toString.toLong))
          }
          s"/data/${list(1)}/origin/${list(2)}/$hdfsTime/data"
        })), "@hdfs_path", ""))

  def process(event: RichMap): RichMap = Try {
    var dst: mutable.Map[String, Any] = new mutable.HashMap[String, Any]()
    dst ++= event
    dst += ("meta_app_name" -> "renren")
    dst += ("meta_table_name" -> "car_price")
    dst = textService.process(dst)
    dst = regexService.process(dst)
    dst = numberService.process(dst)
    dst = filedTypeService.process(dst)
    dst = mergeService.process(dst)
    dst = conditionsService.process(dst)
    RichMap(dst.toMap)
  } match {
    case Success(value) =>
      value
    case Failure(exception) =>
      exception.printStackTrace()
      event
  }

}
