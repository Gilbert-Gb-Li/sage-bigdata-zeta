package com.haima.sage.bigdata.etl.driver

import akka.actor.Actor
import com.haima.sage.bigdata.etl.common.model.{Usability, UsabilityChecker}
import com.haima.sage.bigdata.etl.common.plugin.{Checkable, Plugin}

import scala.util.{Failure, Success}

trait CheckableServer extends Actor {

  def checkup(mate: Checkable): Usability = mate match {
    case mate: Plugin if !mate.installed =>
      Usability(usable = false, cause = s"plugin[${mate.info}] not install".toString)

    case a: FlinkMate =>
      a.usable().map(_.getConstructors.head.newInstance(mate, context).asInstanceOf[UsabilityChecker].check) match {
        case Success(usability) =>
          usability
        case Failure(e) =>
          e.printStackTrace()
          Usability(usable = false, cause = s"checkup driver[${a.name}] fail::${e.getMessage}".toString)

      }
    case a: DriverMate =>
      a.usable().map(_.getConstructors.head.newInstance(mate).asInstanceOf[UsabilityChecker].check) match {
        case Success(usability) =>
          usability
        case Failure(e) =>
          e.printStackTrace()
          Usability(usable = false, cause = s"checkup driver[${a.name}] fail:${e.getMessage}".toString)

      }
    case _ =>
      Usability(usable = false, cause = s"unknown usable ".toString)


  }

  def isInstall(mate: Checkable): Usability = mate match {
    case mate: Plugin if !mate.installed =>
      Usability(usable = false, cause = s"plugin[${mate.info}] not install".toString)
    case _ =>
      Usability()
  }

  def checkup(list: List[Checkable]): Usability = {
    /*没有需要检查的时候 就是可用的*/
    if (list.isEmpty) {
      Usability()
    } else {
      /*有不可用的则不可用*/
      list.find {
        case w: Plugin if !w.installed() =>
          true
        case _ =>
          false
      } match {
        case Some(w: Plugin) =>
          Usability(usable = false, cause = s"plugin[${w.info}] not install".toString)
        case _ =>
          var usability: Usability = null
         if( list.filter(_.isInstanceOf[DriverMate]).exists {
            case w: DriverMate =>
              usability = checkup(w)
              !usability.usable
          }){
           usability
         }else{
           Usability()
         }

      }
    }


  }
}
