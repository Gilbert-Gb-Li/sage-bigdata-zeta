package com.haima.sage.bigdata.etl.performance.service

import java.text.SimpleDateFormat
import java.util.Date

import com.haima.sage.bigdata.etl.common.model.RichMap

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

object GuajiCarParamsUtils {
  val rd = new util.Random
  private val field = "data"
  private val textService = new TextMatchService(field, Seq(
    TextUpConfig("car_name",
      TextFindConfig("基本参数"),
      TextFindConfig("(^\\s?.*\\d{4}款.*)", matchType=false),
      trim = false
    ),
    TextSingleConfig("firm", TextFindConfig("厂商", 1)),
    TextSingleConfig("engine", TextFindConfig("发动机", 1)),
    TextSingleConfig("gear", TextFindConfig("变速箱", 1)),
    TextSingleConfig("structure", TextFindConfig("车身结构", 1)),
    TextSingleConfig("emission", TextFindConfig("排量.*", 1)),
    TextSingleConfig("fuel_type", TextFindConfig("燃料类型", 1)),
    TextSingleConfig("fuel_supply", TextFindConfig("供油方式", 1)),
    TextSingleConfig("emission_standard", TextFindConfig("排放标准", 1)),
    TextSingleConfig("drive", TextFindConfig("驱动方式", 1))
    //    TextSingleConfig("scale", TextFindConfig("级别", 1)),
    //    TextSingleConfig("model", TextFindConfig("证件品牌型号", 1)),
    //    TextSingleConfig("measure", TextFindConfig("长*宽*高(mm)", 1)),
    //    TextSingleConfig("wheelbase", TextFindConfig("轴距(mm)", 1)),
    //    TextSingleConfig("luggage", TextFindConfig("行李箱容积(L)", 1)),
    //    TextSingleConfig("mass", TextFindConfig("整备质量(kg)", 1)),
    //    TextSingleConfig("intake", TextFindConfig("进气形式", 1)),
    //    TextSingleConfig("cylinder", TextFindConfig("气缸", 1)),
    //    TextSingleConfig("power", TextFindConfig("最大马力(Ps)", 1)),
    //    TextSingleConfig("torque", TextFindConfig("最大扭矩(N*m)", 1)),
    //    TextSingleConfig("fuel_rank", TextFindConfig("燃油标号", 1)),
    //    TextSingleConfig("assist", TextFindConfig("助力类型", 1)),
    //    TextSingleConfig("front_suspension", TextFindConfig("前悬挂类型", 1)),
    //    TextSingleConfig("back_suspension", TextFindConfig("后悬挂类型", 1)),
    //    TextSingleConfig("front_brake", TextFindConfig("前制动类型", 1)),
    //    TextSingleConfig("back_brake", TextFindConfig("后制动类型", 1)),
    //    TextSingleConfig("park_brake", TextFindConfig("驻车制动类型", 1)),
    //    TextSingleConfig("front_tyre", TextFindConfig("前轮胎规格", 1)),
    //    TextSingleConfig("back_tyre", TextFindConfig("后轮胎规格", 1)),
    //    TextSingleConfig("main&co-pilot_Airbags", TextFindConfig("主副驾驶安全气囊", 1)),
    //    TextSingleConfig("back_tyre", TextFindConfig("后轮胎规格", 1)),
    //    TextSingleConfig("back_tyre", TextFindConfig("后轮胎规格", 1))
  ),
    attrs = Seq(XmlToTextConfig("text")),
    cleans = Seq("关注\n我要优惠\n电话客服\n在线咨询\n", "我要优惠\n", "车价分析\n")
  )

  private val regexService = new RegexService(
    new RegexConfig("structure",
      "\\d门(\\d+)座(.*)",
      Map[Int, String](1 -> "seats", 2 -> "motor_type")),
    new RegexConfig("schema",
      ".*;link_uid=(\\d+)/.*",
      Map[Int, String](1 -> "car_uid")),
    new RegexConfig("schema",
      ".*S.puid=(.+);.*",
      Map[Int, String](1 -> "car_uid"))
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
      val hdfsPath = s"/data/guaji/origin/car_params/${hdfsTime}/data"
      event += ("@hdfs_path" -> hdfsPath)
    }),
    BaseConditionConfig(event => {
      event.getOrElseUpdate("seats", 0)
    })
  )

  def process(event: RichMap): RichMap = Try {
    var dst: mutable.Map[String, Any] = new mutable.HashMap[String, Any]()
    dst ++= event
    dst += ("meta_app_name" -> "guaji")
    dst += ("meta_table_name" -> "car_params")
    dst = textService.process(dst)
    dst = regexService.process(dst)
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
