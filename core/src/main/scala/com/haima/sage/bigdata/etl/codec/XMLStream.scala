package com.haima.sage.bigdata.etl.codec

import java.io.IOException

import com.haima.sage.bigdata.etl.common.model.{Event, Stream}

import scala.annotation.tailrec
import scala.util.matching.Regex
import scala.util.{Failure, Success, Try}
import scala.xml.XML

/**
  * Created by liyju on 2017/9/12.
  */
object XMLStream {
  def apply(codec: XmlCodec, reader: Stream[Event]) = new XMLStream(codec, reader)

}
class XMLStream(codec: XmlCodec, reader: Stream[Event]) extends Stream[Event](codec.filter) {
  private var header: Option[Map[String, Any]] = None
  private var item: Event = _
  private var flag = false //
  val REG = Array("<\\?.*?\\?>", "<!--.*?>", "<%.*?%>")
  private var root:String = "<>"
  private var end:String="</ >"

  private var event: List[String] = Nil

  @tailrec
  private def nextStr(): String = {
    event match {
      case head :: tails =>
        event = tails
        head
      case Nil =>
        if (reader.hasNext) {
          val e = reader.next()
          if(header.isEmpty)
            header =  e.header
          event = List(e.content)
          nextStr()
        } else {
          // 读取完毕，设置完成状态
          state = State.done
          ""
        }
    }
  }

  def makeData(): Unit = {
    val builder: StringBuffer = new StringBuffer()
    var beforeLength =0 //处理前记录的字符数
    var ignoreLength = 0 //拼接成一个正确的XML记录，忽略的字符数
    var content: String = ""
    val startPattern = new Regex("""<\s*(\w+)([^>]*)>\s*""") //开始节点的正则
    var flag = false   //标识是否构建xml 成功
    while(! "".equals({
      content = nextStr()
      content
    })){
      var startIndex = 0 //开始节点的下标索引
      var intervalStart = 0
      var intervalEnd = 0
      beforeLength = content.length  //记录的原始长度
      for (reg <- REG)
         content = content.replaceAll(reg, "")
      ignoreLength += beforeLength-content.length //正则过滤后忽略的字节数

      //判断是否找到了根节点
      if ("<>".equals(root)) {
        val rootWith = startPattern.findFirstIn(content) //找到第一个开始正则的字符串
        if (rootWith.nonEmpty) {
          root = {
            val start = rootWith.get.replace("<","").replace(">","").trim
            if(start.indexOf(" ")>0){ //判断标签中是否带有属性
              start.substring(0,start.indexOf(" "))
            }else
              start
          }
          startIndex = content.indexOf(rootWith.get) //开始节点的下标索引
          ignoreLength += startIndex
        }else{
          ignoreLength += content.length
        }
      }

      if(! "<>".equals(root)){ //如果确定开始节点，找结束节点
        val endPattern = new Regex(s"</\\s*$root\\s*>\\s*") //结束节点的正则
        val startPatternTemp = new Regex(s"</\\s*($root)([^>]*)\\s*>")
        //如果是非根节点结束标志
        val endWith = endPattern.findFirstIn(content)
        if(endWith.nonEmpty){ //存在结束节点
          val endIndex = content.indexOf(endWith.get)+endWith.get.length
          end = endWith.get.trim
          builder.append(content.substring(startIndex,endIndex))
          val contentTemp = content.substring(startIndex,endIndex)
          //判断结束标志后是否还有其他字符串
          if(endIndex<content.length){
            event=List(content.substring(endIndex))
          }else{
            event=Nil
          }
          intervalStart=startPatternTemp.findAllIn(contentTemp).toList.length

          intervalEnd=endPattern.findAllIn(contentTemp).toList.length


        }else{
          builder.append(content)
        }
      }

      if("<>" != root && end == s"</$root>" &&intervalStart==intervalEnd){
        Try(XML.loadString(builder.toString)) match {
          case Success(_) =>
            ready()
            item = Event(Option(header.getOrElse(Map()) + ("ignoreLength"->ignoreLength)),content= builder.toString)
            flag = true
            root = "<>"
            end="</ >"
            return
          case Failure(_) =>
            ignoreLength=0
            logger.error("MK A XML LINE ERROR")
            state = State.fail
        }
      }
    }
  }
  @tailrec
  final def hasNext: Boolean = {
    state match {
      case State.ready =>
        true
      case State.done =>
        false
      case State.fail =>
        false
      case _ =>
        makeData()
        hasNext
    }
  }

  def next(): Event = {
    init()
    item
  }

  @throws(classOf[IOException])
  override def close() {
    super.close()
    reader.close()
  }

  override def toString(): String = "XMLStream"

}
