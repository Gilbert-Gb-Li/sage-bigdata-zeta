package com.haima.sage.bigdata.etl.utils

import java.util.Date

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.lexer.JSONLogLexer
import javax.script.{Compilable, ScriptEngineManager}
import org.junit.Test

/**
  * Created by zhhuiyan on 2017/3/22.
  */
class ScalaScriptEngineTest {

  @Test
  def split(): Unit ={

   val d= List("a","b","c","d","e","f").zip("a\tb\tc\td\te\tf".split("\t")).toMap

    RichMap(d)
    println(d)
  }




  val manager = new ScriptEngineManager
  val engine = {


    val e = manager.getEngineByName("scala")
    val settings = e.asInstanceOf[scala.tools.nsc.interpreter.IMain].settings
    settings.usejavacp.value = true //使用程序的class path作为engine的class path
    e
  }

  @Test
  def testScala(): Unit = {
    import javax.script.SimpleBindings
    val bindings = new SimpleBindings

    val script = "1+1"
    val rt = engine.asInstanceOf[Compilable].compile(script).eval(bindings)

    println(rt)
  }


  @Test
  def yaojian1(): Unit = {



    val script =
      """import java.util.Date
        |    import java.util.Calendar
        |
        |
        |    import java.util.{Calendar, Date}
        |
        |    val license_num = event.get("license_num"); //资质信息许可证号
        |
        |
        |    val ocr_issue_date = event("ocr_issue_date").toString.toLong; //许可证颁发时间ocr识别
        |    val has_license = event.get("has_license"); //是否上传许可证
        |    val Is_license_vague = event.get("Is_license_vague"); //许可证号是否模糊
        |    val ocr_business_scope = event.get("ocr_business_scope"); //主营业态是否有网络
        |
        |
        |    val org_code = event.get("org_code"); //替换过的org_code
        |
        |    val bad_comment_total_num = event.get("bad_comment_total_num").map(_.asInstanceOf[Int]).getOrElse(0); //差评总数
        |    val comment_total_num = event.get("comment_total_num").map(_.asInstanceOf[Int]).getOrElse(0); //评论总数
        |    val str = "2018-01-01";
        |    val timestamp = new Date().getTime().toString();
        |    val curdate = new Date();
        |
        |    //许可证过期时间
        |    val licexpirydate = event.get("licexpiry").map(t => new Date(t.toString.toLong));
        |    val issue_date = new Date(ocr_issue_date)
        |    val calendar = Calendar.getInstance();
        |    calendar.add(Calendar.YEAR, 2018)
        |    calendar.set(2018, 1, 1, 0, 0, 0)
        |    val issue_time = calendar.getTime
        |
        |    //101,证过期;102,无网络经营资格;111,证号有误;112,未亮证;113,未亮照;114,JY模糊;115,证件模糊;100,正常
        |    val e1 = event ++ (org_code match {
        |      case Some(x: String) if x != null =>
        |        val org_code_2 = x.substring(0, 2);
        |        val org_code_4 = x.substring(0, 4);
        |        val org_code_6 = x.substring(0, 6);
        |        Map("org_code_2" -> org_code_2, "org_code_4" -> org_code_4, "org_code_6" -> org_code_6)
        |      case _ =>
        |        Map()
        |    })
        |
        |
        |    val key_word_total = event.filter(t => {
        |      t._1 == "key_word_hair_num" || t._1 == "key_word_fly_num" || t._1 == "key_word_nfresh_num"
        |    }).map(t => (t._1, t._2.asInstanceOf[Int])).values.sum
        |    val e2 = e1 + ("key_word_total" -> key_word_total)
        |
        |    val bad_comment_rate = if (comment_total_num > 0) {
        |      bad_comment_total_num / comment_total_num;
        |
        |    } else {
        |      0
        |    }
        |    val e3 = e2 + ("bad_comment_rate" -> bad_comment_rate)
        |
        |    def func1(license_num: String): Int = {
        |      //药监库许可证号
        |      event.get("licno") match {
        |        case Some(x: String) if x != null && license_num == x =>
        |          111
        |
        |        case _ =>
        |
        |          licexpirydate match {
        |            case Some(x: Date) if x != null =>
        |              if (x.getTime() < curdate.getTime()) { //证过期
        |                //证过期
        |                101
        |              } else {
        |                if (has_license.isEmpty) { //没上传许可证
        |                  //未亮证
        |                  112
        |                } else {
        |                  if (issue_date.getTime() > issue_time.getTime()) { //2018年1月1日后颁发
        |                    if (ocr_business_scope == 0) { //主体业态没网络
        |                      //无网络经营资格
        |                      102
        |                    } else {
        |                      func2();
        |                    }
        |                  } else {
        |                    func2();
        |                  }
        |                }
        |              }
        |            case _ =>
        |              111
        |          }
        |
        |
        |      }
        |    }
        |
        |
        |    def func2(): Int = { // 方法
        |      //是否上传营业执照
        |      event.get("has_business_license") match {
        |        case Some(x: Int) if x == 0 =>
        |          // 未亮照
        |          113
        |
        |        case _ =>
        |          //正常
        |          100
        |      }
        |    }
        |
        |    /*
        |    *
        |    *  if (has_license == 0 || has_license == 1) { //判断许可证是否上报
        |                1
        |              }
        |    * */
        |    val question_type = license_num match {
        |      //公示信息有jy号
        |      case Some(x: String) if x != null =>
        |
        |        func1(x);
        |      //公示信息无jy号
        |      case _ =>
        |        if (has_license.exists(_.asInstanceOf[Int] == 0)) { //未上传许可证
        |          //未亮证
        |          112
        |
        |        } else {
        |          if (Is_license_vague == 1) { //ocr许可证号模糊
        |            //证号模糊无法识别
        |            114
        |          } else {
        |
        |            func1(null);
        |          }
        |
        |        }
        |    }
        |
        |    e3 + ("question_type" -> question_type)
        |
        |
        |
         """.stripMargin

    val exec =
      s"""
         |import com.haima.sage.bigdata.etl.common.model.RichMap
         |val exec:RichMap=>RichMap=event=>{
         |${script};
         |}
         |exec
         |""".stripMargin

    //    "license_num": "shop_info_no",
    val map = JSONLogLexer().parse(
      s"""{
        |    "Is_license_vague": 1,
        |    "ocr_issue_date":${new Date().getTime},
        |    "c@receive_time":${new Date().getTime},
        |    "has_license": 1,
        |    "ocr_business_scope": 0,
        |    "has_business_license": 1,
        |    "shop_id": "1_0000002",
        |    "ocr_license_num": "shop_info_no",
        |    "template_version": "1.0.0",
        |    "check_date": "",
        |    "waimai_type": "1",
        |    "shop_name": "测试问题类型ocr111",
        |    "shop_cert_photo1": "http://p1.meituan.net/xianfu/10bef063bf2eee677cfe4ababeaff8621334631.jpg",
        |    "question_type": "111",
        |    "key_word_total": 0,
        |    "bad_comment_rate": 0,
        |    "address": "南京市高淳区古柏镇桃源雅居A区茅山路A36号",
        |    "city": "南京市",
        |    "latitude": "31.361628",
        |    "open_time": "09:00-23:30",
        |    "shop_logo": "http://p1.meituan.net/waimaipoi/d8a387fbb31c99f3e3e5f5b40e2bc15d519457.jpg",
        |    "province": "江苏省",
        |    "phone": "13913864384",
        |    "org_code": "320118",
        |    "longitude": "118.940651",
        |    "org_code_4": "3201",
        |    "org_code_6": "320118",
        |    "org_code_2": "32"
        |  }""".stripMargin)



    val compiled = engine.asInstanceOf[Compilable].compile(exec)

   // engine.put("event", map)
   val func= compiled.eval().asInstanceOf[Function1[RichMap,RichMap]]
    val start = System.currentTimeMillis();
    (1 to 2000000) foreach {i=>
      func.apply(map)
    }

    println(s"100000 take time ${System.currentTimeMillis() - start} ms")


  }


}
