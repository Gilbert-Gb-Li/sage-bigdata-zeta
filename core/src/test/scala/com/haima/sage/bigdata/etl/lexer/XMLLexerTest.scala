package com.haima.sage.bigdata.etl.lexer

import org.junit.Test

/**
  * Created by Dell on 2017-08-31.
  */
class XMLLexerTest {


  lazy val lexer = XMLLogLexer()

  @Test
  def xmlWithArray(): Unit = {

    val json =
      """<root><recordInfo>
        |<fieldInfo1>1</fieldInfo1>
        |<fieldInfo>
        |<fieldChName v2="sss">网络IP</fieldChName>
        |<fieldEnName>c60000010</fieldEnName>
        |<fieldContent><![CDATA[10.1.222.140]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName v2="dd">安全域</fieldChName>
        |<fieldEnName>c60000008</fieldEnName>
        |<fieldContent><![CDATA[IT管理区]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName>类型</fieldChName>
        |<fieldEnName>type</fieldEnName>
        |<fieldContent><![CDATA[网络]]></fieldContent>
        |</fieldInfo>
        |</recordInfo><recordInfo>
        |<fieldInfo1>2</fieldInfo1>
        |<fieldInfo>
        |<fieldChName v2="sss">网络IP</fieldChName>
        |<fieldEnName>c60000010</fieldEnName>
        |<fieldContent><![CDATA[10.1.222.140]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName v2="dd">安全域</fieldChName>
        |<fieldEnName>c60000008</fieldEnName>
        |<fieldContent><![CDATA[IT管理区]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName>类型</fieldChName>
        |<fieldEnName>type</fieldEnName>
        |<fieldContent><![CDATA[网络]]></fieldContent>
        |</fieldInfo>
        |</recordInfo><recordInfo>
        |<fieldInfo1>3</fieldInfo1>
        |<fieldInfo>
        |<fieldChName v2="sss">网络IP</fieldChName>
        |<fieldEnName>c60000010</fieldEnName>
        |<fieldContent><![CDATA[10.1.222.140]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName v2="dd">安全域</fieldChName>
        |<fieldEnName>c60000008</fieldEnName>
        |<fieldContent><![CDATA[IT管理区]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName>类型</fieldChName>
        |<fieldEnName>type</fieldEnName>
        |<fieldContent><![CDATA[网络]]></fieldContent>
        |</fieldInfo>
        |</recordInfo><recordInfo>
        |<fieldInfo1>4</fieldInfo1>
        |<fieldInfo>
        |<fieldChName v2="sss">网络IP</fieldChName>
        |<fieldEnName>c60000010</fieldEnName>
        |<fieldContent><![CDATA[10.1.222.140]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName v2="dd">安全域</fieldChName>
        |<fieldEnName>c60000008</fieldEnName>
        |<fieldContent><![CDATA[IT管理区]]></fieldContent>
        |</fieldInfo>
        |<fieldInfo>
        |<fieldChName>类型</fieldChName>
        |<fieldEnName>type</fieldEnName>
        |<fieldContent><![CDATA[网络]]></fieldContent>
        |</fieldInfo>
        |</recordInfo></root>""".stripMargin

    val json2 =
      """<a>
        |   <b>1</b>
        |   <b>2</b>
        |   <b>2</b>
        |</a>""".stripMargin
    val root = lexer.parse(json)("root")
    assert(root.isInstanceOf[java.util.Map[String@unchecked, Any@unchecked]])
    val recordinfo = root.asInstanceOf[java.util.Map[String, java.util.List[Any@unchecked]]].get("recordInfo")
    assert(recordinfo.size() == 4)
    val fieldinfo1 = recordinfo.get(0).asInstanceOf[java.util.Map[String@unchecked, Any@unchecked]]
    val fieldInfos = fieldinfo1.get("fieldInfo")
    assert(fieldInfos.isInstanceOf[java.util.List[Any@unchecked]])
    val fieldInfo = fieldInfos.asInstanceOf[java.util.List[Any@unchecked]].get(0)
    assert(fieldInfo.isInstanceOf[java.util.Map[String@unchecked, Any@unchecked]])

    assert(fieldInfo.asInstanceOf[java.util.Map[String@unchecked, Any@unchecked]].containsKey("fieldContent"))

    val a = lexer.parse(json2)("a")
    assert(a.isInstanceOf[java.util.Map[String@unchecked, Any@unchecked]])
    assert(a.asInstanceOf[java.util.Map[String@unchecked, Any@unchecked]].get("b").isInstanceOf[java.util.List[Any@unchecked]])
    val b = a.asInstanceOf[java.util.Map[String@unchecked, Any@unchecked]].get("b").asInstanceOf[java.util.List[Any@unchecked]]

    import scala.collection.JavaConversions._

    assert(b.containsAll(List[String]("1", "2")), s"data[$json2] must be contains ['1','2']")
    assert(b.size() == 3, s"data[$json2] field[b] size must be 3 ")

  }
@Test
def xmlWithHeader(): Unit ={
  val json=
    """<a><b>1<b><a>""".stripMargin
 println(lexer.parse(json) )
}
}
