package com.haima.sage.bigdata.etl

import scala.util.Try
import com.haima.sage.bigdata.etl.common.model.RichMap

object ParserYYTest extends App {

  def scalaString07(event: RichMap): RichMap = {


    var result = RichMap()
    result ++= event
    val local = event.getOrElse("location", null)
    if (local != null) {
      result += ("location" -> local.toString.split(" ")(0))
    } else {
      result += ("location" -> "unknow")
    }
    val is_live = event.getOrElse("is_live", "false").toString.toBoolean
    val isLiveInt = if (is_live) 1 else 0
    val follow_num = event.getOrElse("follow_num", 0)
      .toString
      .replace("关注", "")
      .replace(".", "")
      .trim
    val followNumInt = Try(follow_num.toInt).getOrElse(0)
    val online_num = event.getOrElse("online_num", 0)
      .toString
      .replace("在线", "")
      .replace(".", "")
      .trim
    val onlineNum = Try(online_num.toInt).getOrElse(0)
    val user_level = event.getOrElse("user_level", 0)
      .toString
      .replace("级", "")
      .replace(".", "")
      .trim
    val userLevel = Try(user_level.toInt).getOrElse(0)
    val fans_num = event.getOrElse("fans_num", 0)
      .toString
      .replace("粉丝", "")
      .replace(".", "")
      .trim
    val fansNum = Try(fans_num.toInt).getOrElse(0)
    val score = event.getOrElse("contribution_score", 0)
      .toString
      .replace("贡献值", "")
      .replace("贡献榜", "")
      .replace(".", "")
      .replace("," ,"")
      .trim
    val scoreInt = Try(score.toInt).getOrElse(0)
    result ++= Map(
      "contribution_score" -> scoreInt,
      "is_live" -> isLiveInt,
      "follow_num" -> followNumInt,
      "online_num" -> onlineNum,
      "user_level" -> userLevel,
      "fans_num" -> fansNum
    )
    result
  }

  def scalaUserInfoTest(): Unit ={
    val str = "{\"timestamp\":\"1547542388192\",\"cloudServiceId\":\"8cb74aff5c2e47e6b1cb543bae164e26\",\"data\":\"[{\\\"online_num\\\":\\\"650在线\\\",\\\"contribution_score\\\":\\\"贡献榜 2,395,020\\\",\\\"live_desc\\\":\\\"双喜兄妹~\\\",\\\"user_label_list\\\":\\\"灵魂歌者,霸道总裁,天籁之音,爽朗直率,实力唱将,+ 添加\\\",\\\"share_url\\\":\\\"https:\\\\/\\\\/www.yy.com\\\\/share\\\\/i\\\\/15180401\\\\/63971038\\\\/63971038\\\\/1547542287?version=7.11.1&edition=1&platform=5&config_id=55&userUid=2361880738\\\",\\\"user_age\\\":\\\"204\\\",\\\"fans_num\\\":\\\"250560粉丝\\\",\\\"follow_num\\\":\\\"4关注\\\",\\\"user_integral\\\":\\\"55790\\\",\\\"location\\\":\\\"中国 • 8天前\\\",\\\"room_id\\\":\\\"71690\\\",\\\"user_name\\\":\\\"兄妹户外商场♪-圆子弹\\\",\\\"last_start_time\\\":\\\"2019-01-15 14:03\\\",\\\"user_level\\\":\\\"472级\\\",\\\"user_id\\\":\\\"10205892\\\",\\\"user_family\\\":\\\"7265\\\",\\\"is_live\\\":true,\\\"data_generate_time\\\":1547542388191}]\",\"appVersion\":\"7.11.1\",\"appPackageName\":\"com.duowan.mobile\",\"dataSource\":\"app\",\"schema\":\"live_user_info\",\"resourceKey\":\"com.duowan.mobile INFO com.yy.mobile.ui.ylink.LiveTemplateActivity 6adbd7b607aa62f40457b482d43ba14f1cbdd0dd\",\"spiderVersion\":\"3.0.25\",\"containerId\":\"08796c7a-5739-481b-8106-f4d443fa5473\",\"dataType\":1}"
    val event = RichMap(Map("user_level" -> "472级"))
    val r = scalaString07(event)
    println(r.get("user_level") match {
      case Some(x) => x
      case None => 1000
    })
  }

  scalaUserInfoTest
}
