package com.haima.sage.bigdata.etl.utils

import com.haima.sage.bigdata.etl.common.Constants

import scala.collection.mutable


/**
  * Created by zhhuiyan on 15/12/29.
  *192.168.0.0 - 192.168.255.255
  *172.16.0.0 - 172.31.255.255
  *10.0.0.0 - 10.255.255.255
  */
object DictionaryTable {
  var person: Map[String, Map[String, String]] = Map()
  val dictionary: scala.collection.mutable.Map[String, Map[String, (String, String)]] = new mutable.LinkedHashMap[String, Map[String, (String, String)]]
  val inners = List(IPLongConvert.to("192.168.0.0") -> IPLongConvert.to("192.168.255.255"),
    IPLongConvert.to("172.16.0.0") -> IPLongConvert.to("172.31.255.255")
    , IPLongConvert.to("10.0.0.0") -> IPLongConvert.to("10.255.255.255"))
  var asset = new mutable.LinkedHashMap[String, Map[String, String]]
  var location: List[Map[String, String]] = List()
  var rules: Map[String, Map[String, String]] = Map()
  var fields: Set[String] = Set()

  try {
    IPExt.load(Constants.CONF.getString("app.store.ip.ext.path"))
  } catch {
    case e: Exception =>
      IPExt.load("ip_location.datx")
  }


  def find(ip: String): Map[String, Any] = {

    val _ip = IPLongConvert.to(ip)
    if (_ip == -1l) {
      Map()
    } else if (inners.exists(tuple => tuple._1 <= _ip && tuple._2 >= _ip)) {
      Map()
    } else {
      dictionary.get(ip) match {
        case Some(data) =>
          val rt = data.map {
            case (key, value) =>
              (key, value._1)
          }

          val lat = rt.getOrElse("lat", "0")
          val lon = rt.getOrElse("lon", "0")
          rt.filterKeys(key =>
            key != "lat" && key != "lon").+("location" -> (lat.toDouble, lon.toDouble))
        //data.filterNot(d => d._1 == "begin_ip_number" || d._1 == "end_ip_number")
        case None =>
          val srcIpInfo: Array[String] = IPExt.find(ip)
          val country: String = srcIpInfo(0) //国家
        val province: String = srcIpInfo(1)
          //城市/省会
          val city: String = srcIpInfo(2) //城市
        val company: String = srcIpInfo(3)
          val latitude: String = srcIpInfo(5) //维度
        val longitude: String = srcIpInfo(6) //经度
        val countryCode: String = srcIpInfo(11)
          //国家简码
          /* val zone: String = if (srcIpInfo(7).indexOf("/") >= 0) {
             srcIpInfo(7).substring(0, srcIpInfo(7).indexOf("/"))
           }
           else {
             srcIpInfo(7)
           }*/
          /*if (("中国" == country) || ("局域网" == country)) {
            city = IPExt.provinceCityMap.get(province)
            countryName = "China"
          }
          else {
            city = srcIpInfo(11)
            if (srcIpInfo(7).indexOf("/") >= 0) {
              countryName = srcIpInfo(7).substring(0, srcIpInfo(7).indexOf("/"))
            }
            else {
              countryName = srcIpInfo(7)
            }
          }*/
          val _latitude = latitude.trim match {
            case l: String if l.matches("\\d*\\.?\\d+") =>
              l.toDouble
            case _ =>
              0
          }
          val _longitude = longitude.trim match {
            case l: String if l.matches("\\d*\\.?\\d+") =>
              l.toDouble
            case _ =>
              0
          }
          val geoPoint = (_latitude, _longitude)

          Map("country" -> country,
            "province" -> province,
            "city" -> city,
            "company" -> company,
            "countryCode" -> countryCode,
            "location" -> geoPoint)

      }
    }
  }

  def getAsset(field: String): Map[String, Any] = {
    asset.get(field) match {
      case Some(data) =>

        data.get("asset_ip") match {
          case Some(ip) =>
            data ++ find(ip).map { case (key, value) => ("asset_" + key, value) }
          case _ =>
            data
        }
      case None =>
        Map()
    }
  }

  def get(field: String, name: String): Map[String, Any] = {
    val datas: Map[String, String] = person.get(field) match {
      case Some(data) =>
        data.filterNot(_._1 == "person_ip").map {
          case (key, value) =>
            (name + "_" + key, value)
        }.+(name + "_type" -> "person")
      case None =>
        asset.get(field) match {
          case Some(data) =>
            data.filterNot(_._1 == "asset_ip").map {
              case (key, value) =>
                (name + "_" + key, value)
            } + (name + "_type" -> "asset")
          case None =>
            Map()
        }
    }
    val ip = IPLongConvert.to(field)
    if (ip != -1l) {
      datas ++ find(field).map {
        case (key, value) =>
          (name + "_" + key, value)
      }

    } else {
      datas
    }


  }

  def rule(field: String): Map[String, String] = {
    rules.get(field) match {
      case Some(data) =>
        data
      case None =>
        Map()
    }
  }
}

case class Dictionary(asset: mutable.LinkedHashMap[String, Map[String, String]] = mutable.LinkedHashMap(),
                      person: Map[String, Map[String, String]] = Map(),
                      location: List[Map[String, String]] = List(),
                      rules: Map[String, Map[String, String]] = Map(),
                      fields: Set[String] = Set())
