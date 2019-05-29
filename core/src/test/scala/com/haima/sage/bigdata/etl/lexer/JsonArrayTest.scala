package com.haima.sage.bigdata.etl.lexer

import com.haima.sage.bigdata.etl.common.model.JsonParser
import com.haima.sage.bigdata.etl.common.model.filter.{Extends, Filter, ReParser}
import org.junit.Test

class JsonArrayTest {
  val data =
    """{
      |"timestamp": "2018-08-14T12:10:31.724Z",
      |"spider_version": "2.0.0",
      |"app_version": "4.8.0",
      |"data": [{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"请叫我场控嘴\",\"content\":\" [img] 神秘人（蒜苗） 你也看了小妖精这么久了\"}]","data_generate_time":"1534248571721","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"此号已退出龙珠\",\"content\":\" [img] 神秘人（五爪金龙） 表哥是大佬\"}]","data_generate_time":"1534248575906","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"请叫我场控嘴\",\"content\":\" [img] 神秘人（蒜苗） 对 979丶开朗 66666\"}]","data_generate_time":"1534248581088","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"请叫我场控嘴\",\"content\":\" [img] 神秘人（蒜苗） 一看就是 48级的\"}]","data_generate_time":"1534248591562","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"林中小怪\",\"content\":\" [img] [img] 林中小怪 我不是大佬\"}]","data_generate_time":"1534248598679","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"此号已退出龙珠\",\"content\":\" [img] 神秘人（五爪金龙） 对\"}]","data_generate_time":"1534248599734","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"林中小怪\",\"content\":\" [img] [img] 林中小怪 看看房管发挥可以\"}]","data_generate_time":"1534248605059","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"孤單的承诺没人愛\",\"content\":\" [img] [img] 孤單的承诺没人愛 房管 是不是 吓跑了\"}]","data_generate_time":"1534248606444","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"林中小怪\",\"content\":\" [img] [img] 林中小怪 吃瓜\"}]","data_generate_time":"1534248609217","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"979丶开朗\",\"content\":\" [img] [img] [img] 979丶开朗 3888我要不起\"}]","data_generate_time":"1534248625389","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"此号已退出龙珠\",\"content\":\" [img] 神秘人（五爪金龙） 房管大佬呢？\"}]","data_generate_time":"1534248626573","search_id":"2649095"}],
      |"app_package_name": "com.longzhu.tga",
      |"schema": "live_danmu",
      |"template_version": "185",
      |"hsn_id": "cad2d4252f83068192cede8c967ad86b0e91d855",
      |"protocol_version": "2.0.0"
      |}""".stripMargin
  val data2 =
    """{
      |"timestamp": "2018-08-14T12:10:31.724Z",
      |"spider_version": "2.0.0",
      |"app_version": "4.8.0",
      |"data": [{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"请叫我场控嘴\",\"content\":\" [img] 神秘人（蒜苗） 你也看了小妖精这么久了\"}]","data_generate_time":"1534248571721","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"此号已退出龙珠\",\"content\":\" [img] 神秘人（五爪金龙） 表哥是大佬\"}]","data_generate_time":"1534248575906","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"请叫我场控嘴\",\"content\":\" [img] 神秘人（蒜苗） 对 979丶开朗 66666\"}]","data_generate_time":"1534248581088","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"请叫我场控嘴\",\"content\":\" [img] 神秘人（蒜苗） 一看就是 48级的\"}]","data_generate_time":"1534248591562","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"林中小怪\",\"content\":\" [img] [img] 林中小怪 我不是大佬\"}]","data_generate_time":"1534248598679","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"此号已退出龙珠\",\"content\":\" [img] 神秘人（五爪金龙） 对\"}]","data_generate_time":"1534248599734","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"林中小怪\",\"content\":\" [img] [img] 林中小怪 看看房管发挥可以\"}]","data_generate_time":"1534248605059","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"孤單的承诺没人愛\",\"content\":\" [img] [img] 孤單的承诺没人愛 房管 是不是 吓跑了\"}]","data_generate_time":"1534248606444","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"林中小怪\",\"content\":\" [img] [img] 林中小怪 吃瓜\"}]","data_generate_time":"1534248609217","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"979丶开朗\",\"content\":\" [img] [img] [img] 979丶开朗 3888我要不起\"}]","data_generate_time":"1534248625389","search_id":"2649095"},{"live_id":"2649095","user_id":"2649095","message_info":"[{\"audience_name\":\"此号已退出龙珠\",\"content\":\" [img] 神秘人（五爪金龙） 房管大佬呢？\"}]","data_generate_time":"1534248626573","search_id":"2649095"}],
      |"app_package_name": "com.longzhu.tga",
      |"schema": "live_danmu",
      |"template_version": "185",
      |"hsn_id": "cad2d4252f83068192cede8c967ad86b0e91d855",
      |"protocol_version": "2.0.0"
      |}""".stripMargin


  @Test
  def jsonLexer(): Unit = {
    val lexer = JSONLogLexer()
    val filter = Filter(Array(Extends("data"),ReParser(Some("message_info"),JsonParser()),Extends("message_info")))

    val  dat=lexer.parse(data)
    println(dat)
    val filtered = filter.filter(dat)
    assert(filtered.size > 1, s"$Extends must be big then 1")
    assert(filtered.head.contains("live_id"), s"$Extends must be contains live_id ")

  }
}
