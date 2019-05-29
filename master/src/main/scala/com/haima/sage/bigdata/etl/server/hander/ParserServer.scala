package com.haima.sage.bigdata.etl.server.hander

import java.util

import akka.http.scaladsl.model.StatusCodes
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.lexer.Delimiter
import com.haima.sage.bigdata.etl.lexer.Flat.flatMap
import com.haima.sage.bigdata.etl.lexer.filter.ReParserProcessor
import com.haima.sage.bigdata.etl.store._
import com.haima.sage.bigdata.etl.store.resolver.ParserStore

/**
  * Created by zhhuiyan on 16/7/18.
  */
class ParserServer extends StoreServer[ParserWrapper, String] with PreviewHandler {
  override val store: ParserStore = Stores.parserStore

  import scala.collection.JavaConversions._

  override def receive: Receive = {
    case (Opt.SYNC, (Opt.UPDATE, data: ParserWrapper)) =>
      if (store.all().exists(ds => ds.name == data.name && !ds.id.contains(data.id.orNull))) {
        sender() ! BuildResult("304", "211", data.name, data.name)
        //sender() ! Result("304", message = s"your set datasource[${data.name}] has exist in system please rename it!")
      } else {
        // TODO 检查数据库是否可用
        // val checkKnowledgeResult = checkKnowledgeColumn(data.parser)
        // if (checkKnowledgeResult._1)
        if (store.set(data)) {
          sender() ! BuildResult("200", "209", data.name)
          //sender() ! Result("200", message = s"update datasource[${data.id}] success, effective after after restart channel that dependence this!")
        } else {
          sender() ! BuildResult("304", "210", data.name)
          //sender() ! Result("304", message = s"update datasource[${data.id}] failure!")
        }
        //        else
        //          sender() ! BuildResult("304", "212", data.name, checkKnowledgeResult._2.keys.toString(), checkKnowledgeResult._2.values.toString())
      }
    case (Opt.SYNC, (Opt.CREATE, data: ParserWrapper)) =>
      if (store.all().exists(_.name == data.name)) {
        sender() ! BuildResult("304", "211", data.name, data.name)
        //sender() ! Result("304", message = s"YOUR SET DATASOURCE[${data.name}] HAS EXIST IN SYSTEM PLEASE RENAME IT!")
      } else {
        if (store.set(data)) {
          sender() ! BuildResult("200", "200", data.name)
          //sender() ! Result("200", message = s"add datasource[${data.id}] success!")
        } else {
          sender() ! BuildResult("304", "201", data.name)
          //sender() ! Result("304", message = s"add datasource[${data.id}] failure!")
        }
      }

    case (Opt.PREVIEW, parserWrapper: ParserWrapper) =>
      val send = sender()
      parserWrapper.sample match {
        case Some(sample) if sample.trim() != "" =>
          val data = parse(sample.split("#next#").toList, parserWrapper.parser.orNull)

          val rtData = data.map(t => flatMap(t).toMap)

          sender() ! (rtData, dataToProperties(data, parserWrapper))
        case _ =>
          parserWrapper.datasource match {
            case Some(id) if id != null =>
              val dataSource = dataSourceStore.get(id)
              dataSource match {
                case Some(source) =>
                  source.collector match {
                    case Some(id) =>{
                      val collector: Unit = collectorStore.get(id) match{
                        case Some(collector)=>{
                          preview(send, collector, (source.data, parserWrapper.parser.orNull), parserWrapper)
                        }
                        case _   =>{
                          sender() ! (List("error" -> s"数据源 ${source.name} 的采集器 ${id} 有误,请重新配置"), List())
                        }
                      }
                    }
                    case _=>{
                      sender() ! (List("error" -> s"数据源 ${source.name} 没有配置采集器"), List())
                    }
                  }
                case _ =>
                      sender() ! (List("error" -> s"数据源 $id 不存在,请重新配置"), List())
              }
            case _ =>
              sender() ! (List("error" -> s"请配置数据源"), List())
          }
      }


    //      val (source, parser, collector) = if (data.id.isDefined && data.parser == null) {
    //        val wrapper = store.get(data.id.orNull)
    //        val dataSource = dataSourceStore.get(wrapper.get.datasource.orNull)
    //
    //        (dataSource.orNull.data, wrapper.get.parser.orNull, )
    //      } else {
    //
    //        val dataSource = dataSourceStore.get(data.datasource.orNull)
    //        logger.debug(s"parser preview[$data] in ds[$dataSource]")
    //        (dataSource.orNull.data, data.parser.orNull, collectorStore.get(dataSource.get.collector.get).get)
    //
    //      }


    case (Opt.SYNC, (Opt.DELETE, id: String)) =>
      sender() ! delete(id)
    case obj =>
      super.receive(obj)

  }


  def previewer(json: String): List[Object] = {
    mapper.readValue[(List[String], Parser[MapRule])](json) match {
      case (Nil, _) =>
        List()
      case (data: List[String], parser) =>
        parse(data, parser)
    }

  }


  def getDelimitCount(event: String, length: Int, delimit: Delimiter): Int = {
    val record: util.List[String] = new util.ArrayList[String]
    var tail = event
    val separate = delimit.delimit.get
    val separate_len = separate.length
    var i = tail.indexOf(separate)
    var result = length
    while (i != -1 && record.size() < length - 1) {
      var head = tail.substring(0, i)
      var heads = head.toCharArray
      while (i != -1 && delimit.inQuote(heads, '\'')) {
        i = tail.indexOf(separate, i + separate_len)
        if (i != -1) {
          head = tail.substring(0, i)
          result = result - 1
          heads = head.toCharArray
        }
      }
      while (i != -1 && delimit.inQuote(heads, '\"')) {
        i = tail.indexOf(separate, i + separate_len)
        if (i != -1) {
          head = tail.substring(0, i)
          result = result - 1
          heads = head.toCharArray
        }
      }
      if (i != -1) {
        tail = tail.substring(i + separate_len)
        i = tail.indexOf(separate)
      }
    }
    result
  }


  def parse(data: List[String], parser: Parser[MapRule]): List[Map[String, Any]] = {
    parser match {
      case d: Delimit if d.fields == null || d.fields.isEmpty =>
        val delimit = Delimiter(d.delimit, 100)
        data.map(line => {
          var count = 1
          line.toCharArray.foreach { c =>
            if (c == d.delimit.get.charAt(0))
              count += 1
          }
          delimit.length = getDelimitCount(line, count, delimit)
          var i: Int = -1
          delimit.parse(line).map(value => {
            i = i + 1
            ("field" + i, value)
          }).toMap
        })

      case _ =>
        val analyzer = ReParserProcessor(ReParser(parser = parser))
        val dd = data.flatMap(line => analyzer.parse(Map("c@raw" -> line)))
        toConvert(dd)
    }
  }


  def delete(id: String): BuildResult = {
    val parser = store.get(id).orNull

    configStore.byParser(id) match {
      //被通道使用
      case head :: _ =>
        BuildResult("304", "217", parser.name, head.name)
      case Nil =>
        store.bySub(id) match {
          //被其他规则引用
          case head :: _ =>
            BuildResult("304", "216", parser.name, head.name)
          case Nil =>
            knowledgeStore.byParser(id) match {
              //被知识库引用
              case head :: _ =>
                BuildResult("304", "219", parser.name, head.name.orNull)
              case Nil =>
                if (store.delete(id)) {
                  logger.info(s"DELETE PARSER[$id] SUCCEED!")
                  //Result(StatusCodes.OK.intValue.toString, "DELETE PARSER SUCCESS")
                  BuildResult(StatusCodes.OK.intValue.toString, "204", parser.name)
                  //Result(StatusCodes.OK.intValue.toString, s"DELETE PARSER[$id] SUCCEED")
                } else {
                  BuildResult(StatusCodes.NotAcceptable.intValue.toString, "205", parser.name)
                  //Result(StatusCodes.NotAcceptable.intValue.toString, s"DELETE PARSER[$id] FAILED")
                }
            }
        }
    }
  }

  def checkKnowledgeColumn(parser: Option[Parser[MapRule]]): (Boolean, Map[String, String]) = {
    //添加filter是知识库补充的情况下，知识库字段的校验
    parser match {
      case Some(rule) =>
        if (rule.filter != null && rule.filter.nonEmpty && rule.filter.exists(_.isInstanceOf[ByKnowledge])) {
          val byKnowledgeArray =
            rule.filter.filter(_.isInstanceOf[ByKnowledge])
              .map(_.asInstanceOf[ByKnowledge]).find(
              byKnowledge => {
                val table = "KNOWLEDGE_" + byKnowledge.id.replace("-", "_").toUpperCase()
                !knowledgeStore.getColumns(table).contains(byKnowledge.column.toUpperCase)
              })
          if (byKnowledgeArray.nonEmpty) {
            (false, byKnowledgeArray.map(byKnowledge =>
              (knowledgeStore.get(byKnowledge.id) match {
                case Some(knowledge) =>
                  knowledge.name.getOrElse("")
                case None =>
                  ""
              }, byKnowledge.column)).toMap)
          } else
            (true, Map[String, String]())
        } else
          (true, Map[String, String]())

      case None => (true, Map[String, String]())
    }
  }
}
