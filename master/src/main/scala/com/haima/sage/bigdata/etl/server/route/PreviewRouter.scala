package com.haima.sage.bigdata.etl.server.route

import java.util
import java.util.regex.{Matcher, Pattern}

import akka.actor.{Actor, Props}
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import com.haima.sage.bigdata.etl.common.Implicits._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.common.model.filter._
import com.haima.sage.bigdata.etl.lexer.filter.ReParserProcessor
import com.haima.sage.bigdata.etl.lexer.{Delimiter, Flat}
import com.haima.sage.bigdata.etl.server.hander.ParserServer
import io.swagger.annotations._
import javax.ws.rs.Path

import scala.collection.mutable
import scala.concurrent.Future
import scala.util.parsing.json.{JSON, JSONObject}
import scala.util.{Failure, Success, Try}
import scala.xml.XML

/**
  * Created by zhhuiyan on 7/2/2015.
  */
@Api(value = "预览", produces = "application/json")
@Path("/previewer/")
class PreviewRouter(override val context: akka.actor.ActorSystem) extends DefaultRouter {

  import Flat._

  private lazy val parserServer = context.actorOf(Props[ParserServer])
  var list: List[JSONObject] = _

  @ApiOperation(value = "解析规则预览", notes = "预览相关操作", httpMethod = "put")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "是否美化", readOnly = false,
      dataType = "query", dataTypeClass = classOf[String]),
    new ApiImplicitParam(name = "body", value = "预览相关数据", readOnly = false,
      dataType = "form", dataTypeClass = classOf[ParserWrapper])
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "预览", response = classOf[Pagination[ParserWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  private def previewer: Route = pathEndOrSingleSlash {
    entity(as[String]) {
      json =>
        parameter('pretty.?) {
          pretty =>
            complete {
              try {
                previewData(json)
              } catch {
                case ex: Exception =>
                  logger.error("GET HEALTH INFO ERROR", ex)
                  toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "902", ex.getMessage), pretty)
                //toJson(Result(StatusCodes.InternalServerError.intValue.toString, s"Api service error : ${ex.getMessage}"), pretty)
              }
            }
        }
    }

  }

  @ApiOperation(value = "开始解析规则预览", notes = "预览相关操作", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "是否美化", readOnly = false,
      dataType = "query", dataTypeClass = classOf[String]),
    new ApiImplicitParam(name = "body", value = "预览相关数据", readOnly = false,
      dataType = "form", dataTypeClass = classOf[ParserWrapper])
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "预览", response = classOf[Pagination[ParserWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("")
  private def startPreviewer: Route = pathEndOrSingleSlash {
    entity(as[String]) {
      json =>
        parameter('pretty.?) {
          pretty =>
            val config = mapper.readValue[ParserWrapper](json)
            onComplete[(List[Map[String, Any]], List[ParserProperties])] {
              parserServer.?(Opt.PREVIEW, config)(previewTimeout).asInstanceOf[Future[(List[Map[String, Any]], List[ParserProperties])]]
            } {
              case Success((value, props)) =>
                complete(toJson(Array(props, value), pretty))
              case Failure(ex) =>
                complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
              //complete(StatusCodes.InternalServerError, s"""{"message"="An error occurred: ${ex.getMessage}"}""".stripMargin)

            }


          /*complete {
            toJson(previewProperties(json), pretty)
          }*/
        }

    }
  }


  @ApiOperation(value = "分析规则预览", notes = "显示预览相关操作", httpMethod = "get")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "是否美化", readOnly = false,
      dataType = "query", dataTypeClass = classOf[String])
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "预览", response = classOf[Pagination[ParserWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("preview")
  private def preview: Route = {
    parameter('pretty.?) {
      pretty =>
        complete {
          try {
            listData()
          } catch {
            case ex: Exception =>
              logger.error("GET HEALTH INFO ERROR", ex)
              toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "902", ex.getMessage), pretty)
            //toJson(Result(StatusCodes.InternalServerError.intValue.toString, s"Api service error : ${ex.getMessage}"), pretty)

          }
        }
    }
  }

  @ApiOperation(value = "开始分析规则预览", notes = "显示预览数据相关操作", httpMethod = "put")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "是否美化", readOnly = false,
      dataType = "query", dataTypeClass = classOf[String]),
    new ApiImplicitParam(name = "body", value = "提前预览相关数据", readOnly = false,
      dataType = "form", dataTypeClass = classOf[ParserWrapper])
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "提前预览", response = classOf[Pagination[ParserWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("")
  private def startPreview: Route =
    entity(as[String]) {
      json =>
        parameter('pretty.?) {
          pretty =>
            complete {
              try {
                previewData(json)
              } catch {
                case ex: Exception =>
                  logger.error("GET HEALTH INFO ERROR", ex)
                  toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "902", ex.getMessage), pretty)
                //toJson(Result(StatusCodes.InternalServerError.intValue.toString, s"Api service error : ${ex.getMessage}"), pretty)
              }
            }
        }
    }

  @ApiOperation(value = "删除预览", notes = "删除预览", httpMethod = "delete")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "是否美化", readOnly = false,
      dataType = "query", dataTypeClass = classOf[String])
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "删除成功", response = classOf[Pagination[ParserWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("")
  private def _delete = delete {
    parameter('pretty.?) {
      pretty =>
        complete {
          try {
            remove()
          } catch {
            case ex: Exception =>
              logger.error("GET HEALTH INFO ERROR", ex)
              toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "902", ex.getMessage), pretty)
            //toJson(Result(StatusCodes.InternalServerError.intValue.toString, s"Api service error : ${ex.getMessage}"), pretty)
          }
        }
    }


  }

  @ApiOperation(value = "显示分析数据", notes = "显示分析数据相关操作", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "是否美化", readOnly = false,
      dataType = "query", dataTypeClass = classOf[String]),
    new ApiImplicitParam(name = "body", value = "提前预览相关数据", readOnly = false,
      dataType = "form", dataTypeClass = classOf[Pagination[ParserWrapper]])
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "提前预览"),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("")
  private def previewParser: Route = entity(as[String]) {
    json =>
      parameter('pretty.?) {
        pretty =>
          logger.info(s"PREVIEW parser[$json]")
          val config = mapper.readValue[ParserWrapper](json)
          onComplete[(List[Map[String, Any]], List[_])] {
            logger.info(s"PREVIEW datasource[$config]")
            (parserServer ? (Opt.PREVIEW, config.copy(parser = config.parser match {
              case Some(d: Delimit) if d.fields == null || d.fields.isEmpty =>
                Some(TransferParser(filter = d.filter))
              case obj =>
                obj
            }))).asInstanceOf[Future[(List[Map[String, Any]], List[_])]]
          } {
            case Success(value) =>
              val data = config.parser match {
                case Some(d: Delimit) if d.fields == null || d.fields.isEmpty =>
                  val delimit = Delimiter(d.delimit, 100)
                  value._1.map(_.getOrElse("c@raw", "").toString()).map(line => {
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
                  value._1.map(line => flatMap(line.filterNot(_._1.startsWith("_"))))
              }
              complete(toJson(data, pretty))
            case Failure(ex) =>
              complete(toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "901", ex.getMessage), pretty))
          }
      }

  }

  @ApiOperation(value = "显示探测结果", notes = "探测器相关操作", httpMethod = "post")
  @ApiImplicitParams(Array(
    new ApiImplicitParam(name = "pretty", value = "是否美化", readOnly = false,
      dataType = "query", dataTypeClass = classOf[String]),
    new ApiImplicitParam(name = "body", value = "提前预览相关数据", readOnly = false,
      dataType = "form", dataTypeClass = classOf[ParserWrapper])
  ))
  @ApiResponses(Array(
    new ApiResponse(code = 200, message = "提前预览", response = classOf[Pagination[ParserWrapper]]),
    new ApiResponse(code = 500, message = "Internal server error")
  ))
  @Path("detect")
  private def detect: Route = entity(as[String]) {
    json =>
      parameter('pretty.?) {
        pretty =>
          complete {
            try {
              toJson(detecte(mapper.readValue[List[String]](json)), pretty)
            } catch {
              case ex: Exception =>
                logger.error("detecte date has error", ex)
                toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "902", ex.getMessage), pretty)
              //toJson(Result(StatusCodes.InternalServerError.intValue.toString, s"Api service error : ${ex.getMessage}"), pretty)
            }

          }
      }
  }

  //
  //  @ApiOperation(value = "带样本预览", notes = "在有样本的情况下预览", httpMethod = "post")
  //  @ApiImplicitParams(Array(
  //    new ApiImplicitParam(name = "pretty", value = "是否美化", readOnly = false,
  //      dataType = "query", dataTypeClass = classOf[String]),
  //    new ApiImplicitParam(name = "body", value = "提前预览相关数据", readOnly = false,
  //      dataType = "form", dataTypeClass = classOf[ParserWrapper])
  //  ))
  //  @ApiResponses(Array(
  //    new ApiResponse(code = 200, message = "提前预览", response = classOf[Pagination[ParserWrapper]]),
  //    new ApiResponse(code = 500, message = "Internal server error")
  //  ))
  //  @Path("previewWithSample")
  //  private def previewWithSample: Route = entity(as[String]) {
  //    json =>
  //      parameter('pretty.?) {
  //        pretty =>
  //          complete {
  //            try {
  //              val params = mapper.readValue[(List[String], ParserWrapper)](json)
  //              val data = parseData(params._1, params._2.parser.get)
  //              toJson(Array(dataToProperties(data, params._2), data), pretty)
  //            } catch {
  //              case ex: Exception =>
  //                logger.error("preview date has error", ex)
  //                toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString, "902", ex.getMessage), pretty)
  //              //toJson(Result(StatusCodes.InternalServerError.intValue.toString, s"Api service error : ${ex.getMessage}"), pretty)
  //            }
  //
  //          }
  //      }
  //  }


  def route: Route = {
    pathPrefix("previewer") {
      put {
        previewer
      } ~
        post {
          startPreviewer
        }
    } ~
      pathPrefix("preview") {
        get {
          preview
        } ~
          put {
            startPreview
          } ~
          post {
            previewParser
          } ~
          _delete
      } ~
      pathPrefix("detect") {
        post {
          detect
        }
      } //~
    //      pathPrefix("previewWithSample") {
    //        post {
    //          previewWithSample
    //        }
    //      }

  }


  def previewer(json: String): List[Object] = {
    mapper.readValue[(List[String], Parser[MapRule])](json) match {
      case (Nil, _) =>
        List()
      case (data: List[String], parser) =>
        parse(data, parser)
    }

  }

  def parse(data: List[String], parser: Parser[MapRule]): List[Map[String, Any]] = {
    import scala.collection.JavaConversions._
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
        data.flatMap(line => analyzer.parse(Map("raw" -> line)).map(t => flatMap(t).toMap))
    }
  }

  class PreviewServer extends Actor {
    override def receive: Receive = {
      case (config: Config, "preview") =>
        context.actorSelection("/user/server") forward(config.collector.get.id, (config, "preview"))
      case l: List[_] =>
        list = l.asInstanceOf[List[JSONObject]]
    }
  }

  private lazy val server = context.actorOf(Props.create(classOf[PreviewServer], this))

  def previewData(json: String): String = {
    val config: Config = mapper.readValue[Config](json)
    config.collector match {
      case Some(_) =>
        server ! (config, "preview")
        "your request is received"

      case None =>
        "your request is received,but not processed not found collector"
    }
  }

  def listData(): String =
    if (list == null) {
      " please waiting some times for server get result or your provide  none for preview !"
    } else {
      list.toString()
    }


  def remove(): String = {
    list = null
    " cache clear ok !"
  }

  def detecte(samplesList: List[String]): Parser[MapRule] = {
    detecteParserType(samplesList) match {
      case ("json", "", "") => JsonParser()
      case ("xml", "", "") => XmlParser()
      case ("delimit", delimit, "") => Delimit(Some(delimit), Array())
      case ("delimitWithKeyMap", delimit, tab) => DelimitWithKeyMap(Some(delimit), Some(tab))
      case _ => NothingParser()
    }
  }

  /**
    * 获取parser type
    *
    * @param samplesList 样本日志列表
    * @return Tuple(String,String,String)  (类型，分隔符,键值对分隔符)
    */
  def detecteParserType(samplesList: List[String]): (String, String, String) = {
    val resultMap = new mutable.HashMap[(String, String, String), Int]()
    samplesList.foreach(item =>
      JSON.parseFull(item) match {
        //判断是否是json格式
        case Some(_) => resultMap.put(("json", "", ""), resultMap.getOrElse(("json", "", ""), 0) + 1)
        case None =>
          Try(XML.loadString(item)) match { //判断是都是xml格式
            case Success(_) => resultMap.put(("xml", "", ""), resultMap.getOrElse(("xml", "", ""), 0) + 1)
            case Failure(_) => //判断是都是分隔符或是键值对的格式
              val specialCharList = item.toCharArray.toList.filter(isSpecialChar)
              times(specialCharList).sortBy(_._2) match {
                //默认是分隔符格式
                case Nil => resultMap.put(("nothing", "", ""), resultMap.getOrElse(("nothing", "", ""), 0) + 1)
                case head :: Nil => resultMap.put(("delimit", head._1.toString, ""), resultMap.getOrElse(("delimit", head._1.toString, ""), 0) + 1)
                case _data =>
                  val head = _data.last
                  val second = _data.init.last
                  if (item.split(head._1).length == item.split(head._1).map(field => if (field.contains(second._1)) 1 else 0).sum) {
                    resultMap.put(("delimitWithKeyMap", head._1.toString, second._1.toString), resultMap.getOrElse(("delimitWithKeyMap", head._1.toString, second._1.toString), 0) + 1)
                  } else if (item.split(second._1).length == item.split(second._1).map(field => if (field.contains(head._1)) 1 else 0).sum) {
                    resultMap.put(("delimitWithKeyMap", second._1.toString, head._1.toString), resultMap.getOrElse(("delimitWithKeyMap", second._1.toString, head._1.toString), 0) + 1)
                  } else {
                    resultMap.put(("delimit", head._1.toString, ""), resultMap.getOrElse(("delimit", head._1.toString, ""), 0) + 1)
                  }
              }
          }
      })
    resultMap.toList.maxBy(_._2)._1
  }

  /**
    * 判断是否是特殊字符
    *
    * @param str 字符
    * @return Boolean
    */
  def isSpecialChar(str: Char): Boolean = {
    val regEx = "[ _`~!@#$%^&*()+=|{}':;',\\[\\].<>/?~！@#￥%……&*（）——+|{}【】‘；：”“’。，、？]|\n|\r|\t"
    val p: Pattern = Pattern.compile(regEx)
    val m: Matcher = p.matcher(str.toString)
    m.find()
  }

  /**
    * 计算每个字符出现的次数
    *
    * @param chars 特殊字符列表
    * @return List[(Char, Int)]
    */
  def times(chars: List[Char]): List[(Char, Int)] = {
    var res: mutable.Map[Char, Int] = mutable.Map()
    for (char: Char <- chars) {
      if (res.contains(char)) res.update(char, res(char) + 1) else res += (char -> 1)
    }
    res.toList
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

}
