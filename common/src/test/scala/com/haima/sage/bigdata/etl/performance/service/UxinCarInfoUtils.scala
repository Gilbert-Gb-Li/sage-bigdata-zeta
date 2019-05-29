package com.haima.sage.bigdata.etl.performance.service

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object UxinCarInfoUtils {
  private val field = "data"
  private val textService = new TextMatchService(field, Seq(
    TextSingleConfig("check_date",
      TextFindConfig("车辆检测员", offset = 1),
      pattern = Some("\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}")),

    TextSingleConfig("price", TextFindConfig("([\\d\\.]+)万")),
    TextUpConfig("car_name",
      TextFindConfig("车辆档案"),
      TextFindConfig("\\d{4}款", firstGroup = false, matchType = false)
    ),

    TextBetweenConfig("certificate_tmp",
      TextFindConfig("\\d{4}款", offset = 1, firstGroup = false, matchType = false),
      TextFindConfig("车辆档案", -1)
    ),
    TextSingleConfig("share_link", TextFindConfig("shared-link=.+(?<data>https://.+)")),
    TextSingleConfig("lift_time", TextFindConfig("提车时间", 1)),
    TextSingleConfig("car_id", TextFindConfig("编号：(\\d+)")),

    TextSingleConfig("registration_date", TextFindConfig("(\\d{4}年\\d{2}月)上牌")),
    TextSingleConfig("car_color", TextFindConfig("颜色", 1)),
    TextSingleConfig("apparent_mileage", TextFindConfig("表显里程", 1)),
    TextSingleConfig("engine_intake", TextFindConfig("表显里程", 2)),
    TextSingleConfig("engine_exhaust", TextFindConfig("表显里程", 3)),
    TextSingleConfig("emission_standards", TextFindConfig("排放标准", 1)),

    TextSingleConfig("check_people", TextFindConfig("车辆检测员", -2)),

    TextSingleConfig("last_maintenance_tmp", TextFindConfig("维保记录", 1)),
    TextSingleConfig("fix_times", TextFindConfig("维修次数", 1)),
    TextSingleConfig("maintenance_times", TextFindConfig("保养次数", 1)),
    TextSingleConfig("accident_times", TextFindConfig("事故次数", 1))
  ),
    attrs = Seq(XmlToTextConfig("o"), XmlToTextConfig("shared-link", withName = true)),
    cleans = Seq("关注\n我要优惠\n电话客服\n在线咨询\n", "我要优惠\n", "车价分析\n")
  )
  private val replaceService = ReplaceService(
    ReplaceConfig("apparent_mileage", "公里"),
    ReplaceConfig("fix_times", "次"),
    ReplaceConfig("maintenance_times", "次"),
    ReplaceConfig("accident_times", "次"),
    ReplaceConfig("engine_exhaust", "L"),
    ReplaceConfig("registration_date", "上牌")
  )

  private val regexService = new RegexService(
    new RegexConfig("certificate_tmp",
      "首付([0-9\\.]*?)万\\s*月供([0-9\\.]*?)元",
      Map[Int, String](1 -> "down_payment", 2 -> "month_payment")),
    new RegexConfig("schema", "link_uid=(\\d*)",
      Map[Int, String](1 -> "car_id")
    ),
    RegexConfig("car_name", RegexLogicType.AND, Seq(
      RegexFindConfig("([^\\s]*?)\\s(.*?)((20|19|21)\\d{2})款",
        Map[Int, String](1 -> "brand", 2 -> "mode", 3 -> "year")),
      RegexFindConfig("(手动|自动|纯电动)(.*)",
        Map[Int, String](2 -> "version"))
    )),
    new RegexConfig("last_maintenance_tmp", "(\\d{4}-\\d{1,2}-\\d{1,2})",
      Map[Int, String](1 -> "last_maintenance_date")
    ),
    new RegexConfig("lift_time", "(\\d+)-(\\d+)",
      Map[Int, String](1 -> "lift_time_min", 2 -> "lift_time_max")
    ),

    new RegexConfig("certificate_tmp", "由(.*)仓",
      Map[Int, String](1 -> "car_warehouse")),
    new RegexConfig("certificate_tmp", "优信(.*?)认证",
      Map[Int, String](1 -> "certificate")),
    new RegexConfig("share_link", ".*(https://.*)",
      Map[Int, String](1 -> "share_link"))
  )
  private val numberService = new NumberService(
    NumberItemConfig("apparent_mileage", "wan"),
    NumberItemConfig("price", "wan"))
  private val filedTypeService = FiledTypeService(
    FiledTypeConfig("registration_date", "date", "yyyy年MM月"),
    FiledTypeConfig("last_maintenance_date", "date", "yyyy-MM-dd"),
    FiledTypeConfig("check_date", "date", "yyyy-MM-dd HH:mm:ss"),
    FiledTypeConfig("timestamp", "date", "yyyy-MM-dd HH:mm:ss"),
    FiledTypeConfig("month_payment", "int"),
    FiledTypeConfig("down_payment", "int"),
    FiledTypeConfig("price", "int"),
    FiledTypeConfig("year", "int"),
    FiledTypeConfig("nationwide_purchase", "int"),
    FiledTypeConfig("special_offer", "int"),
    FiledTypeConfig("return_car_3_days", "int")
  )
  private val mergeService = MergeService(MergeConfig(
    "@es_id", Seq("meta_app_name", "meta_table_name", "car_id")
  ))

  def process(event: RichMap): RichMap = Try {
    var dst: mutable.Map[String, Any] = new mutable.HashMap[String, Any]()
    dst ++= event
    dst += ("meta_app_name" -> "uxin")
    dst += ("meta_table_name" -> "car_info")
    dst = textService.process(dst)
    dst = replaceService.process(dst)
    dst = regexService.process(dst)
    dst = numberService.process(dst)
    dst = filedTypeService.process(dst)
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
