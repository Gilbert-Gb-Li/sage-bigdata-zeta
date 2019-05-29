package com.haima.sage.bigdata.etl.server.route

import akka.actor.Props
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import com.haima.sage.bigdata.etl.common.model._
import com.haima.sage.bigdata.etl.store.Stores
import com.haima.sage.bigdata.etl.utils.Logger

import scala.collection.immutable.::

/**
  * Created by zhhuiyan on 2015/6/18.
  */
trait RulesRouter extends DefaultRouter {

  private lazy final val server = context.actorOf(Props[RulesServer])

  val rules = {
    parameter('pretty.?) {
      pretty =>

        pathPrefix("rules") {
          put {
            // URI:rules/_type(export or import) -d 'data file path'
            entity(as[String]) {
              directery => {
                path("export") {

                  complete {
                    server ! ("export", directery)
                    toJson(BuildResult(StatusCodes.OK.intValue.toString,"913"),pretty)
                    //toJson(Result(StatusCodes.OK.intValue.toString, "Request received"), pretty)
                  }
                } ~
                  path("import") {
                    // URI:rulebank/data/_type(export or import) -d 'data file path'
                    complete {
                      server ! ("import", directery)
                      toJson(BuildResult(StatusCodes.OK.intValue.toString,"913"),pretty)
                      //toJson((StatusCodes.OK.intValue.toString, "Request received"), pretty)
                    }
                  }
              }
            }
          } ~ get {
            // -XGET URI:rules/_id
            path(Segment) {
              id =>
                complete {
                  try {
                    toJson(load(id), pretty)
                  } catch {
                    case ex: Exception =>
                      logger.error("RuleBank error", ex)
                      toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"902",ex.getMessage),pretty)
                      //toJson(Result(StatusCodes.InternalServerError.intValue.toString, s"Api service error : ${ex.getMessage}"), pretty)
                  }
                }
              // -XGET URI:rules</>
            } ~ pathEndOrSingleSlash {
              complete {
                try {
                  toJson(load(), pretty)
                } catch {
                  case ex: Exception =>
                    logger.error("RuleBank error", ex)
                    toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"902",ex.getMessage),pretty)
                    //toJson(Result(StatusCodes.InternalServerError.intValue.toString, s"Api service error : ${ex.getMessage}"), pretty)
                }
              }
            }
          } ~ delete {
            // -XDELETE URI:rules/<_id>
            path(Segment) {
              id =>
                complete {
                  try {
                    toJson(remove(id), pretty)
                  } catch {
                    case ex: Exception =>
                      logger.error("rules error", ex)
                      toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"902",ex.getMessage),pretty)
                      //toJson(Result(StatusCodes.InternalServerError.intValue.toString, s"Api service error : ${ex.getMessage}"), pretty)
                  }
                }
            } ~
              // -XPOST URI:rules   -d '<json data for rules>'
              post {
                entity(as[String]) {
                  json =>
                    complete {
                      try {
                        toJson(add(json), pretty)
                      } catch {
                        case ex: Exception =>
                          logger.error("RuleBank post error", ex)
                          toJson(BuildResult(StatusCodes.InternalServerError.intValue.toString,"902",ex.getMessage),pretty)
                          //toJson(Result(StatusCodes.InternalServerError.intValue.toString, s"Api service error : ${ex.getMessage}"), pretty)
                      }
                    }
                }
              }
          }
        }~path("pre-check"){
          post {
            entity(as[String]) {
              json =>
                val flag=  try {
                  val config = mapper.readValue[Rules](json)
                  true
                }catch {
                  case e:Exception=>
                    false
                }
                complete(s"""{"usable":${flag},"message":"上传文件不匹配"}""")

            }
          }
        }
    }
  }

  def load(_id: String): AnyRef = {

    try {


      Stores.rulesStore.get(_id) match {
        case Some(group) =>
          group
        case None =>
          BuildResult(StatusCodes.NotFound.intValue.toString,"917")
          //Result(StatusCodes.NotFound.intValue.toString, s"Not found ruleGroup[${_id}]")

      }
    } catch {
      case ex: Exception =>
        logger.error("Get ruleBank info error", ex)
        BuildResult(StatusCodes.InternalServerError.intValue.toString,"902",ex.getMessage)
        //Result(StatusCodes.InternalServerError.intValue.toString, s"API Service error : ${ex.getMessage}")
    }

  }

  def load(): AnyRef = {

    try {


      Stores.rulesStore.all() match {
        case head :: tail =>
          head :: tail
        case Nil =>
          BuildResult(StatusCodes.NotFound.intValue.toString,"918")
          //Result(StatusCodes.NotFound.intValue.toString, s"Not found rules")

      }
    } catch {
      case ex: Exception =>
        logger.error("Get ruleBank info error", ex)
        BuildResult(StatusCodes.InternalServerError.intValue.toString,"902",ex.getMessage)
        //Result(StatusCodes.InternalServerError.intValue.toString, s"API Service error : ${ex.getMessage}")
    }
  }

  private def remove(_id: String): AnyRef = {
    logger.info(s"RuleBank delete request[_id=${_id}]")
     try {

      _id match {
        case "_all" =>
          Stores.rulesStore.groups().foreach {
            g =>
              Stores.rulesStore.deleteGroup(g.id)
          }
          BuildResult(StatusCodes.OK.intValue.toString,"919")
          //Result(StatusCodes.OK.intValue.toString, s"Delete all success")
        case id: String =>
          Stores.rulesStore.delete(_id)
          if (Stores.rulesStore.delete(_id)) {
            BuildResult(StatusCodes.OK.intValue.toString,"920",_id)
            //Result(StatusCodes.OK.intValue.toString, s"delete rules[${_id}] success")
          } else {
            BuildResult(StatusCodes.OK.intValue.toString,"921",_id)
            //Result(StatusCodes.NotModified.intValue.toString, s"Delete rules[${_id}] failed may not exits")
          }
        case oo =>
          BuildResult(StatusCodes.BadRequest.intValue.toString,"922",oo.toString)
          //Result(StatusCodes.BadRequest.intValue.toString, s"The request type[${oo}] is not supported")
      }
    } catch {
      case ex: Exception =>
        logger.error("Get ruleBank info error", ex)
        BuildResult(StatusCodes.InternalServerError.intValue.toString,"902",ex.getMessage)
        //Result(StatusCodes.InternalServerError.intValue.toString, s"API Service error : ${ex.getMessage}")
    }
  }

  private def add(json: String): AnyRef = {

    logger.info(s"RuleBank post request receive json: $json")
    try {

      val rules = mapper.readValue[Rules](json)
      Stores.rulesStore.get(rules.id) match {
        case Some(g) =>
          BuildResult(StatusCodes.NotAcceptable.intValue.toString,"925",rules.name,rules.id)
          //Result(StatusCodes.NotAcceptable.intValue.toString, s"Rules[${rules.id}] already exist")

        case None =>
          if (Stores.rulesStore.add(rules)) {
            BuildResult(StatusCodes.OK.intValue.toString,"923",rules.name,rules.id)
            //Result(StatusCodes.OK.intValue.toString, s"add Rules[${rules.id}] success")

          } else {
            BuildResult(StatusCodes.NotImplemented.intValue.toString,"924",rules.name,rules.id)
            //Result(StatusCodes.NotImplemented.intValue.toString, s"add Rules[${rules.id}] failed")
          }
      }

    } catch {
      case ex: Exception =>
        logger.error("add Rules info error", ex)
        BuildResult(StatusCodes.InternalServerError.intValue.toString,"902",ex.getMessage)
        //Result(StatusCodes.InternalServerError.intValue.toString, s"API Service error : ${ex.getMessage}")
    }

  }
}

class RulesServer extends akka.actor.Actor with Logger {
  override def receive: Receive = {
    case ("export", path: String) =>
      `import`(path)
    case ("import", path: String) =>
      export(path)
    case msg =>
      logger.warn(s"unknown msg[$msg] for RuleBankActor ")
  }

  def export(json: String): String = {
    logger.info(s"RuleBank export to: $json")
    try {
      Stores.rulesStore.output(json)
    } catch {
      case e: Exception =>
        logger.warn(s"Request[path=$json] failed")
    }
    ""
  }

  def `import`(json: String): String = {
    logger.info(s"RuleBank import from : $json")
    try {
      Stores.rulesStore.input(json)
    } catch {
      case e: Exception =>
        logger.warn(s"Request[path=$json] failed")
    }
    ""
  }
}