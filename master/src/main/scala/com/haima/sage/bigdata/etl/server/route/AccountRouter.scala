/*
package com.haima.sage.bigdata.etl.server.route

import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import com.haima.sage.bigdata.etl.store.auth.{LoginUser, UserStore}

import scala.util.{Failure, Success}

/**
  * Created by zhhuiyan on 15/11/25.
  */
trait AccountRouter extends DefaultRouter {

  private val store = UserStore
  val account = {
    // URI config/collector

    parameter('pretty.?) {
      pretty => {
        pathPrefix("account") {
          path(Segment) {
            id => {
              get {
                onComplete[Option[LoginUser]](store.query(id)) {
                  case Success(value) => complete(toJson(value, pretty))
                  case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")

                }
              } ~ delete {
                onComplete[Int](store.delete(id)) {
                  case Success(value) =>
                    if (value > 0) {
                      complete(s"deleted $id success")
                    } else {
                      complete(s"deleted $id failure")
                    }

                  case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")

                }
              }
            }
          } ~
            pathEndOrSingleSlash {
              put {
                entity(as[String]) {
                  json => {
                    val user = mapper.readValue[LoginUser](json)
                    onComplete[Int](store.add(user)) {
                      case Success(value) =>
                        if (value > 0) {
                          complete(s"add  ${user.name} success")
                        } else {
                          complete(s"add ${user.name} failure")
                        }
                      case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")

                    }
                  }
                }
              } ~ post {
                entity(as[String]) {
                  json => {
                    val user = mapper.readValue[LoginUser](json)
                    onComplete[Int](store.update(mapper.readValue[LoginUser](json))) {
                      case Success(value) => if (value > 0) {
                        complete(s"update ${user.name} success")
                      } else {
                        complete(s"update ${user.name} failure")
                      }
                      case Failure(ex) => complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")

                    }
                  }
                }
              } ~ get {
                onComplete[Seq[LoginUser]](store.query()) {
                  case Success(value) =>
                    complete(toJson(value, pretty))

                  case Failure(ex) =>


                    complete(StatusCodes.InternalServerError, s"An error occurred: ${ex.getMessage}")

                }

              }
            }
        }
      }
    }

  }
}
*/
