package com.haima.sage.bigdata.etl.performance.service

import java.text.SimpleDateFormat
import java.util.Date
import java.util.regex.Pattern

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object GuajiCarInfoUtils {
  val rd = new util.Random
  private val field = "data"
  private val textService = new TextMatchService(field, Seq(
    TextDownConfig("car_name",
      TextFindConfig("车源号\\s*(.+)"), // 车主报价，详情
      TextFindConfig("(^\\s?.*\\d{4}款.*)", matchType=false),
      trim = false
    ),
    TextSingleConfig("source_id", TextFindConfig("车源号\\s*(.+)")),
    TextSingleConfig("price", TextFindConfig("^万$", -1)),
    TextSingleConfig("state", TextFindConfig("car_service_status=(\\d)")),
    TextSingleConfig("state_tmp", TextFindConfig(".*(没有找到相关数据).*")),
    TextSingleConfig("state_tmp", TextFindConfig(".*(全部车源信息).*")),
    TextSingleConfig("service_subtitle", TextFindConfig("服务保障", 1)),
    TextSingleConfig("registration_date", TextFindConfig("上牌时间", 1)),
    TextSingleConfig("apparent_mileage", TextFindConfig("表显里程", 1)),
    TextSingleConfig("plate_address", TextFindConfig("车牌所在地", 1)),
    TextSingleConfig("car_area", TextFindConfig("看车地点", 1)),
    TextSingleConfig("car_area", TextFindConfig("看车方式", 1))
  ),
    attrs = Seq(XmlToTextConfig("text"), XmlToTextConfig("car_service_status", withName=true)),
    cleans = Seq("关注\n我要优惠\n电话客服\n在线咨询\n", "我要优惠\n", "车价分析\n"),
    trim = false
  )
  private val replaceService = ReplaceService(
    ReplaceConfig("apparent_mileage", "公里")
  )

  private val regexService = new RegexService(
    new RegexConfig("service_subtitle",
      "服务费约{0,1}(\\d+)元.*",
      Map[Int, String](1 -> "service_charge")),
    new RegexConfig("service_subtitle",
      "服务费不超过(\\d+)%.*",
      Map[Int, String](1 -> "service_percentage")),
    new RegexConfig("schema",
      ".*;link_uid=(.+)/.*",
      Map[Int, String](1 -> "car_uid")),
    new RegexConfig("schema",
      ".*S.puid=(.+);.*",
      Map[Int, String](1 -> "car_uid"))
  )

  private val numberService = new NumberService(
    NumberItemConfig("apparent_mileage", "wan"),
    NumberItemConfig("price", "wan")
  )

  private val baseConditionService = BaseConditionService(
    BaseConditionConfig(event => {
      val registration_date = event.getOrElse("registration_date", "unknown").toString
      val pattern = "\\d{4}-\\d{1,2}".r
      pattern.findFirstIn(registration_date) match {
        case Some(x) => event += ("registration_date" -> x)
        case None => }
    }),
    BaseConditionConfig(event => {
      val car_name = event.getOrElse("car_name", "").toString
      val pattern = "^\\s.*".r
      pattern.findFirstIn(car_name) match {
        case Some(_) => event += ("is_strict" -> 1)
        case None => event += ("is_strict" -> 0)}
    }),
    BaseConditionConfig(event => {
      val value: String = event.getOrElse("car_uid", "").toString
      if (value.nonEmpty) {
        event += ("car_uid" -> value.replaceAll("/.*",""))
      } else {
        event += ("car_uid" -> s"${rd.nextInt(10000)}unknown")
      }
    }),
    BaseConditionConfig(event => {
      val value: String = event.getOrElse("service_subtitle", "").toString
      val pattern = Pattern.compile("服务费不超过(\\d+)%.*")
      val m = pattern.matcher(value)
      if (m.find()){
        val p = m.group(1).toInt
        val v = event.getOrElse("price", 0).toString.toInt * p / 100
        event += ("service_charge" -> v)
      }
    }),
    BaseConditionConfig(event => {
      var data_generate_time = event.getOrElse("timestamp",
        new Date().getTime.toString)
      val hdfsTime = new SimpleDateFormat("yyyy-MM-dd/HH")
        .format(new Date(data_generate_time.toString.toLong))
      val hdfsPath = s"/data/guaji/origin/car_info/$hdfsTime/data"
      event += ("@hdfs_path" -> hdfsPath)
    }),
    BaseConditionConfig(event => {
      val state = event.getOrElse("state", "").toString
      if (state.isEmpty){
        val state_tmp = event.getOrElse("state_tmp", "").toString
        if (state_tmp.contains("没有找到相关数据")) {
          event += ("state" -> 9)
        } else if (state_tmp.contains("全部车源信息")) {
          event += ("state" -> -1)
        }
      }
    }),
    BaseConditionConfig(event => {
      event.getOrElseUpdate("service_percentage", 0)
      event.getOrElseUpdate("service_charge", 0)
      event.getOrElseUpdate("price", 0)
      event.getOrElseUpdate("is_new", 0)
    })
  )

  private val mergeService = MergeService(
    MergeConfig("@es_id", Seq("meta_app_name", "meta_table_name", "car_uid"))
  )

  def process(event: RichMap): RichMap = Try {
    var dst: mutable.Map[String, Any] = new mutable.HashMap[String, Any]()
    dst ++= event
    dst += ("meta_app_name" -> "guaji")
    dst += ("meta_table_name" -> "car_info")
    dst = textService.process(dst)
    dst = replaceService.process(dst)
    dst = regexService.process(dst)
    dst = numberService.process(dst)
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