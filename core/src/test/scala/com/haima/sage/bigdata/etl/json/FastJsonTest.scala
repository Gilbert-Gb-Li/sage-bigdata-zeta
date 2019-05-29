package com.haima.sage.bigdata.etl.json

import java.util.Date

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.junit.Test

/**
  * Created by zhhuiyan on 16/7/28.
  */
class FastJsonTest {
  /*import com.jsoniter.JsonIterator

JsonIterator.deserialize("[1,2,3]", classOf[Array[Int]])*/

  @Test
  def convert(): Unit ={
    import scala.collection.JavaConversions._

    val log :Map[String,Any]= Map("a" -> "v1", "b" -> "v2", "time" -> new Date())
    val map:java.util.HashMap[String,Any]=new java.util.HashMap()
    log.foreach{
      case (key,value)=>
        map.put(key,value)
    }
    println(map.getClass)
    println(JSON.toJSONString(map, SerializerFeature.WriteClassName))
    JSON.parseObject(JSON.toJSONString(map, SerializerFeature.WriteClassName)).foreach{
      case (key,value)=>
        println(value.getClass)
    }
  }
  @Test
  def parse(): Unit ={
    val data ="""{"APP_NAME":"lpcmp","TIME_COST":34,"REQUEST":{"ISUTIM":"113919","PRCCOD":"PLDESQTL",
                |  "ISUDAT":"20161115",
                |  "INFBDY":{"PLDESQTLX1":[{
                |    "CUST_NM":"","STATUS":"YSYJJ",
                |    "MVMT_TEL_NBR":""}],
                |    "PLSPECUSER":[{
                |    "BRANCH_NAME":"æ·±å³åè¡","VORG_NAME":"æ·±å³åè¡",
                |      "USER_ID":"01029491","VORG_LEVEL":"30",
                |      "USER_LEVEL":"5","USER_NAME":"éåæµè¯",
                |      "VORG_CODE":"755","BRANCH_ID":"755",
                |      "SUBBRANCH_ID":"755083",
                |      "SUBBRANCH_NAME":"æ·±å³åè¡å¬å¸é¶è¡é¨"}],
                |    "PLUSERINFO":[{"BRANCH_NAME":"æ·±å³åè¡",
                |      "USER_IP":"99.6.149.157","VORG_NAME":"æ·±å³åè¡",
                |      "USER_ID":"01029491","VORG_LEVEL":"30","USER_LEVEL":"5",
                |      "USER_NAME":"éåæµè¯","VORG_CODE":"755",
                |      "BRANCH_ID":"755","SUBBRANCH_ID":"755083",
                |      "SUBBRANCH_NAME":"æ·±å³åè¡å¬å¸é¶è¡é¨",
                |      "APP_ID":"lpcmppad"}],
                |    "PLRECOSEND":[{"SENDTAG":"Y",
                |      "BACKNUMBERS":16,"STARTRECORD":{"startline":"33"}}]}},
                |  "REQUEST_TIME":"2016-11-15 11:39:19",
                |  "ENCODING":"GBK","REQUEST_IP":"99.6.149.157",
                |  "WORKBAR_ID":"PLDESQTL","RESPONSE":{"TARSVR":"","WEBCOD":"",
                |  "ISUTIM":"113919","RTNCOD":"SUC0000","PRCCOD":"PLDESQTL",
                |  "ISUDAT":"20161115","ERRMSG":"SUC0000,Successfully",
                |  "INFBDY":{"PLRECOSEND":[{"SENDTAG":"Y","BACKNUMBERS":"16",
                |    "STARTRECORD":"{\"startline\":\"49\"}"}],
                |    "PLDESQTLZ1":[{"CUST_NM":"ææ","MKTTSKNBR":"MKT_2016101820339341",
                |      "CTFTYP":"P01","CTFCOD":"440302198707075661","PRDTYP_DSP":"ä¿¡ç¨ç±»",
                |      "PAD_PIID":"PAD_2016101810044063","PRDTYP":"02","APPLY_TIME":"2016-10-18 10:17:21",
                |      "STATUS_DSP":"é¢å®¡å·²æç»","STATUS":"YSYJJ","MVMT_TEL_NBR":"13333333333",
                |      "DES_NBR":"DES_2016101810139059"},{"CUST_NM":"ãæ±é·åèªå¨åã546296",
                |      "MKTTSKNBR":"MKT_2016101320336275","CTFTYP":"P01","CTFCOD":"156467031947022820",
                |      "PRDTYP_DSP":"æµæ¼ç±»","PAD_PIID":"PAD_2016101310041028","PRDTYP":"01",
                |      "APPLY_TIME":"2016-10-13 17:51:10","STATUS_DSP":"é¢å®¡å·²æç»","STATUS":"YSYJJ",
                |      "MVMT_TEL_NBR":"45616272205","DES_NBR":"DES_2016101310136028"},{"CUST_NM":"ãæ±é·åèªå¨åã613250",
                |      "MKTTSKNBR":"MKT_2016101320336273","CTFTYP":"P01","CTFCOD":"274590405980938430",
                |      "PRDTYP_DSP":"æµæ¼ç±»","PAD_PIID":"PAD_2016101310041026",
                |      "PRDTYP":"01","APPLY_TIME":"2016-10-13 17:51:02","STATUS_DSP":"é¢å®¡å·²æç»",
                |      "STATUS":"YSYJJ","MVMT_TEL_NBR":"34766874990","DES_NBR":"DES_2016101310136026"},
                |      {"CUST_NM":"ãæ±é·åèªå¨åã444952","MKTTSKNBR":"MKT_2016101320336271",
                |        "CTFTYP":"P01","CTFCOD":"658576536269868300","PRDTYP_DSP":"æµæ¼ç±»","PAD_PIID":"PAD_2016101310041024","PRDTYP":"01","APPLY_TIME":"2016-10-13 17:50:57","STATUS_DSP":"é¢å®¡å·²æç»","STATUS":"YSYJJ","MVMT_TEL_NBR":"20772090729",
                |        "DES_NBR":"DES_2016101310136024"},{"CUST_NM":"ãæ±é·åèªå¨åã478387","MKTTSKNBR":"MKT_2016101320336260","CTFTYP":"P01","CTFCOD":"452681683606420700","PRDTYP_DSP":"æµæ¼ç±»",
                |        "PAD_PIID":"PAD_2016101310041016","PRDTYP":"01","APPLY_TIME":"2016-10-13 17:15:31","STATUS_DSP":"é¢å®¡å·²æç»","STATUS":"YSYJJ","MVMT_TEL_NBR":"1623712990","DES_NBR":"DES_2016101310136016"},{"CUST_NM":"ãæ±é·åèªå¨åã594132",
                |        "MKTTSKNBR":"MKT_2016101320336259","CTFTYP":"P01","CTFCOD":"229713395728031300","PRDTYP_DSP":"æµæ¼ç±»",
                |        "PAD_PIID":"PAD_2016101310041015","PRDTYP":"01","APPLY_TIME":"2016-10-13 17:13:58","STATUS_DSP":"é¢å®¡å·²æç»","STATUS":"YSYJJ","MVMT_TEL_NBR":"48145912591","DES_NBR":"DES_2016101310136015"},{
                |        "CUST_NM":"å¯æµ©è1010","MKTTSKNBR":"MKT_2016101020331247","CTFTYP":"P02","CTFCOD":"123456",
                |        "PRDTYP_DSP":"ä¿¡ç¨ç±»","PAD_PIID":"PAD_2016101010036008","PRDTYP":"02","APPLY_TIME":"2016-10-10 14:49:02","STATUS_DSP":"é¢å®¡å·²æç»","STATUS":"YSYJJ","MVMT_TEL_NBR":"15666666666","DES_NBR":"DES_2016101010131003"},{"CUST_NM":"å¯æµ©è","MKTTSKNBR":"MKT_2016100920324407",
                |        "CTFTYP":"P02","CTFCOD":"1234566","PRDTYP_DSP":"ä¿¡ç¨ç±»","PAD_PIID":"PAD_2016100910030021","PRDTYP":"02","APPLY_TIME":"2016-10-09 15:30:35","STATUS_DSP":"é¢å®¡å·²æç»","STATUS":"YSYJJ","MVMT_TEL_NBR":"15998852255","DES_NBR":"DES_2016100910125010"},{"CUST_NM":"è£è¶","MKTTSKNBR":"MKT_2016093020323237",
                |        "CTFTYP":"P01","CTFCOD":"330523199107011818","PRDTYP_DSP":"æµæ¼ç±»","PAD_PIID":"PAD_2016093010028005","PRDTYP":"01","APPLY_TIME":"2016-09-30 11:36:38","STATUS_DSP":"é¢å®¡å·²æç»","STATUS":"YSYJJ","MVMT_TEL_NBR":"18205820957","DES_NBR":"DES_2016093010124002"},{"CUST_NM":"jack","MKTTSKNBR":"MKT_2016092920321229","CTFTYP":"P01","CTFCOD":"440302198707075661","PRDTYP_DSP":"ä¿¡ç¨ç±»","PAD_PIID":"PAD_2016092910026010","PRDTYP":"02","APPLY_TIME":"2016-09-29 21:14:26","STATUS_DSP":"é¢å®¡å·²æç»","STATUS":"YSYJJ","MVMT_TEL_NBR":"12345678905","DES_NBR":"DES_2016092910123003"},{"CUST_NM":"ææ","MKTTSKNBR":"MKT_2016092920316224","CTFTYP":"P01","CTFCOD":"430421198605042685","PRDTYP_DSP":"ä¿¡ç¨ç±»","PAD_PIID":"PAD_2016092910023002","PRDTYP":"02","APPLY_TIME":"2016-09-29 11:04:02","STATUS_DSP":"é¢å®¡å·²æç»","STATUS":"YSYJJ","MVMT_TEL_NBR":"13600007777","DES_NBR":"DES_2016092910120002"},{"CUST_NM":"çå®å¤","MKTTSKNBR":"MKT_2016092020292240","CTFTYP":"P01","CTFCOD":"210202198911284219","PRDTYP_DSP":"ä¿¡ç¨ç±»","PAD_PIID":"DES_2016092010106013","PRDTYP":"02","APPLY_TIME":"2016-09-20 15:13:41","STATUS_DSP":"é¢å®¡å·²æç»","STATUS":"YSYJJ","MVMT_TEL_NBR":"12345678901","DES_NBR":"DES_2016092010106013"},{"CUST_NM":"ææ","MKTTSKNBR":"MKT_2016090720268222","CTFTYP":"P01","CTFCOD":"123","PRDTYP_DSP":"æµæ¼ç±»","PAD_PIID":"DES_2016090710095000","PRDTYP":"01","APPLY_TIME":"2016-09-07 14:54:24","STATUS_DSP":"é¢å®¡å·²æç»","STATUS":"YSYJJ","MVMT_TEL_NBR":"13333333333","DES_NBR":"DES_2016090710095000"},{"CUST_NM":"ææ","MKTTSKNBR":"MKT_2016090120260237","CTFTYP":"P01","CTFCOD":"123","PRDTYP_DSP":"æµæ¼ç±»","PAD_PIID":"DES_2016090110088012","PRDTYP":"01","APPLY_TIME":"2016-09-01 09:49:27","STATUS_DSP":"é¢å®¡å·²æç»","STATUS":"YSYJJ","MVMT_TEL_NBR":"13333333333","DES_NBR":"DES_2016090110088012"},{"CUST_NM":"è£è¶","MKTTSKNBR":"MKT_2016081120209234","CTFTYP":"P01","CTFCOD":"330523199107011818","PRDTYP_DSP":"æµæ¼ç±»","PAD_PIID":"DES_2016081110047003","PRDTYP":"01","APPLY_TIME":"2016-08-11 17:26:40","STATUS_DSP":"é¢å®¡å·²æç»","STATUS":"YSYJJ","MVMT_TEL_NBR":"18603061036","DES_NBR":"DES_2016081110047003"},{"CUST_NM":"è£è¶","MKTTSKNBR":"MKT_2016081120209233","CTFTYP":"P01","CTFCOD":"330523199107011818","PRDTYP_DSP":"æµæ¼ç±»","PAD_PIID":"DES_2016081110047002","PRDTYP":"01","APPLY_TIME":"2016-08-11 17:22:47","STATUS_DSP":"é¢å®¡å·²æç»",
                |  "STATUS":"YSYJJ","MVMT_TEL_NBR":"18603061036",
                |  "DES_NBR":"DES_2016081110047002"}]},"DALCOD":"","RTNLVL":""},
                |  "RETURN_CODE":"SUC0000",
                |  "REQUEST_URL":"http://lpcmppad.paas.cmbchina.cn/Lpcmppad/web/preApprove/preApproveList.html?v=201611150931",
                |  "INDEX_SUFFIX":"1611"}""".stripMargin


    println(JSON.parseObject(data).toJSONString)
  }



}
