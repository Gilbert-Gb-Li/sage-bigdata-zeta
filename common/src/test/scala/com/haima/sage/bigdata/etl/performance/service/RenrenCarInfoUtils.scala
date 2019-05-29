package com.haima.sage.bigdata.etl.performance.service

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object RenrenCarInfoUtils {
  private val hdfsFormat = new SimpleDateFormat("yyyy-MM-dd/HH")
  private val field = "data"
  private val textService = new TextMatchService(field, Seq(
    TextUpConfig("car_name",
      TextFindConfig(".*服务费：.*"),
      TextFindConfig("(^\\s?.*\\d{4}款.*)", matchType=false),
      trim = false
    ),
    TextSingleConfig("source_id", TextFindConfig("车辆编号：(.+)")),
    TextSingleConfig("state_tmp", TextFindConfig(".*(这辆车已经卖掉).*")),
    TextSingleConfig("price", TextFindConfig(".*卖家标价：(\\d+\\.?\\d*).*")),
    TextSingleConfig("service_subtitle", TextFindConfig(".*服务费：.*", 1)),
    TextSingleConfig("registration_date", TextFindConfig("上牌时间", 1)),
    TextSingleConfig("apparent_mileage", TextFindConfig("表显里程", 1)),
    TextSingleConfig("plate_address", TextFindConfig("车牌所在地", 1)),
    TextSingleConfig("car_area", TextFindConfig("看车地点", 1))

    // TextSingleConfig("car_gear", TextFindConfig("变速箱", 1)),
    // TextSingleConfig("emission_standards", TextFindConfig("迁入地为准", 1)), // 排放标准
    // TextSingleConfig("engine_exhaust", TextFindConfig("排放量", 1)),
    // TextSingleConfig("change_times", TextFindConfig("登记证为准", 1)),  // 过户次数
    // TextSingleConfig("annual_inspection_expiry", TextFindConfig("年检到期", 1)), // 年检
    // TextSingleConfig("jqx_expiry", TextFindConfig("交强险到期", 1)),  // 交强险
    // TextSingleConfig("commercial_insurance", TextFindConfig("商业险到期", 1))  // 交强险
  ),
    attrs = Seq(XmlToTextConfig("text")),
    cleans = Seq("关注\n我要优惠\n电话客服\n在线咨询\n", "我要优惠\n", "车价分析\n"),
    trim = false
  )
  private val replaceService = ReplaceService(
    ReplaceConfig("apparent_mileage", "公里")
    // ReplaceConfig("change_times", "次过户"),
    // ReplaceConfig("engine_exhaust", "L")
    // ReplaceConfig("commercial_insurance", "已过期", "1970.1")
  )

  private val regexService = new RegexService(
    new RegexConfig("service_subtitle",
      "(\\d+)元(\\d)%.*",
      Map[Int, String](1 -> "service_charge")),
    new RegexConfig("schema",
      ".*;link_uid=(.*)/.*",
      Map[Int, String](1 -> "car_uid"))
  )
  private val numberService = new NumberService(
    NumberItemConfig("apparent_mileage", "wan"),
    NumberItemConfig("price", "wan")
  )
  private val filedTypeService = FiledTypeService(
    FiledTypeConfig("date_time", "date", "yyyy-MM-dd"),
    FiledTypeConfig("price", "int")
  )
  val rd = new util.Random
  private val conditionsService = ConditionsService(
    ConditionsConfig(Prediction("car_name",  "^\\s.*"), result="is_strict", value=1),
    ConditionsConfig(Prediction(), result="service_percentage", value=0),
    ConditionsConfig(Prediction(), result="is_strict", value=0),
    ConditionsConfig(Prediction(), result="is_new", value=0),
    ConditionsConfig(Prediction("state_tmp",  ".*这辆车已经卖掉.*"),
      result="state", value=2),
    ConditionsConfig(
      Prediction("service_subtitle", "服务费不超过(\\d+)%\\.*"),
      Some(Calculations(List("service_subtitle","price"),
        list=>{
          val m = Pattern.compile("服务费不超过(\\d+)%\\.*").matcher(list.head.toString)
          if(m.find()) m.group(1).toInt * list(1).toString.toInt / 100
        })), "service_charge", 0),
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
        })), "@hdfs_path", ""),
    ConditionsConfig(
      Prediction(condition=true),
      Some(Calculations(List("car_uid"),
        list=>{
          if (list.head!=null) {
            list.head.toString.replaceAll("/.*", "")
          }else {list(1)}
        })), "car_uid", s"${rd.nextInt(10000)}unknown"))

  private val mergeService = MergeService(
    MergeConfig("@es_id", Seq("meta_app_name", "meta_table_name", "car_uid"))
  )
  def process(event: RichMap): RichMap = Try {
    var dst: mutable.Map[String, Any] = new mutable.HashMap[String, Any]()
    dst ++= event
    dst += ("meta_app_name" -> "renren")
    dst += ("meta_table_name" -> "car_info")
    containOperateBar(dst)
    dst = textService.process(dst)
    dst = replaceService.process(dst)
    dst = regexService.process(dst)
    dst = numberService.process(dst)
    dst = filedTypeService.process(dst)
    dst = conditionsService.process(dst)
    dst = mergeService.process(dst)
    RichMap(dst.toMap)
  } match {
    case Success(value) =>
      value
    case Failure(exception) =>
      exception.printStackTrace()
      event
  }

  def containOperateBar(event: mutable.Map[String, Any]): Unit ={
    val data = event.getOrElse("data", null)
    if (data != null && data.asInstanceOf[String].contains("detail_page_bottom_operate_bar")){
      event += ("state" -> 0)
    } else {
      event += ("state" -> 1)
    }
  }

}
