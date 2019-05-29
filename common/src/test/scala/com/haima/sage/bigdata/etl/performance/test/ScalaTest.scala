import java.text.SimpleDateFormat
import java.util.Date

import com.haima.sage.bigdata.etl.common.model.RichMap
import org.apache.commons.lang3.mutable.Mutable

import scala.collection.mutable


object ScalaTest {

  def main(args: Array[String]): Unit = {
//    val event = RichMap(Map("user_id" -> "直播间号:1710359698",
//      "user_name" -> "hehe\\thaha",
//      "live_desc" -> "活着真好\n\n17786215230vx"
//    ))
//    val event2 = dateToTimestamp(event)
//    event2.foreach(e => println(e._1,e._2))
    val a = 1558190671304L
    val b = 1558190670152L
    println(b-a)

  }

  def dateToTimestamp(event: RichMap): RichMap = {
    import java.text.SimpleDateFormat
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    var dst: mutable.Map[String, Any] = new mutable.HashMap[String, Any]()
    val time = event.getOrElse("timestamp", "2019-03-27 00:00:02").toString
    if (time.isEmpty) {
      dst += ("timestamp" -> System.currentTimeMillis())
    } else {
      dst += ("timestamp" -> format.parse(time).getTime)
      dst += ("date_time" -> dst.getOrElse("timestamp", ""))
    }
    event ++ RichMap(dst.toMap)
  }
  def func (b: Boolean, f: Boolean => Int): Unit = {
    println(f(b))
  }

  def eventProcess(event: RichMap): RichMap ={
    import java.text.SimpleDateFormat
    import java.util.Date
    import scala.collection.mutable
    def contentTrim(value: Map[String, Any]): mutable.Map[String, String] ={
      var result = new mutable.HashMap[String, String]()
      value.foreach(v=> {
        result += (v._1 ->
          v._2.toString.trim
            .replace("\t", "")
            .replace("\r", "")
            .replace("\n", ""))
      })
      result
    }
    def idTrim(value: Map[String, Any]): mutable.Map[String, String] ={
      var result = new mutable.HashMap[String, String]()
      value.foreach(v=> {
        result += (v._1 ->
          v._2.toString.trim
            .replace("@","")
            .replace(".",""))
      })
      result += ("user_id" ->
        result.getOrElse("user_id", "")
          .replace("直播间号:", ""))
    }

    val schema = event.getOrElse("schema", "")
    var data_generate_time = event.getOrElse("data_generate_time",
      event.getOrElse("timestamp",
        new Date().getTime.toString
      ))
    val hdfs_time = new SimpleDateFormat("yyyy-MM-dd/HH")
      .format(new Date(data_generate_time.toString.toLong))
    val hdfs_path = s"/data/yy/origin/$schema/$hdfs_time/data"
    val es_Path = "live_r_" + hdfs_time.split("/")(0)
      .replace("-", "")

    // 去除特殊字符
    val toTrim = Map(
      "user_name" -> event.getOrElse("user_name", ""),
      "live_desc" -> event.getOrElse("live_desc", ""),
      "user_label_list" -> event.getOrElse("user_label_list", ""),
      "location" -> event.getOrElse("location", ""),
      "classfication" -> event.getOrElse("classfication", ""),
      "audience_name" -> event.getOrElse("audience_name", ""),
      "gift_name" -> event.getOrElse("gift_name", ""),
      "content" -> event.getOrElse("content", ""))
    val ids = Map(
      "user_id" -> event.getOrElse("user_id", ""),
      "gift_id" -> event.getOrElse("gift_id", ""),
      "audience_id" -> event.getOrElse("audience_id", ""),
      "room_id" -> event.getOrElse("room_id", ""),
      "live_id" -> event.getOrElse("live_id", ""))

    event ++ Map("@hdfsPath" -> hdfs_path,
      "data_generate_time" -> data_generate_time,
      "@es_path" -> es_Path
    ) ++ contentTrim(toTrim).toMap ++ idTrim(ids).toMap


  }




}


case class Person(name: String, career: String)

class ConditionsConfigs {

  var p: Prediction = Prediction()
  var index: String = _
  var value: Any = _
  var Map: Map[String, Any]= _


  def this(default: Map[String, Any]){
    this()
    this.Map = default
  }

    def this( p: Prediction,
              index: String,
              value: Any
             ){
      this()
      this.p = p; this.index = index; this.value = value
    }

  def process(event: mutable.Map[String, Any],
              p: Prediction,
              index: String,
              value: Any): Unit ={
  }

}

case class Prediction(indices: Seq[String] = Nil, condition: Boolean = true)