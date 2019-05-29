package com.haima.sage.bigdata.etl.store.slick

import com.haima.sage.bigdata.etl.common.Constants
import com.haima.sage.bigdata.etl.store.User
import com.haima.sage.bigdata.etl.utils.Logger
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.util.Success

/*/**
  * Created by zhhuiyan on 15/9/2.
  */
object DBCreateUtils extends Logger {
  override val loggerName = "com.haima.sage.bigdata.etl.store.slick.DBCreateUtils"

  def tables = List(TableQuery[Users])

  def create(): Unit = {

    val connect = Database.forConfig("store.driver", ConfigFactory.parseString(s"""store.driver.url="jdbc:derby:${Constants.storePath}derby-user;create=true"""")
      .withFallback(
        Constants.MASTER))

    connect.run(MTable.getTables(None, None, None, None)).onComplete {
      case Success(data) =>
        val names = data.map(_.name.name)
        tables.foreach { table =>
          if (!names.exists(_.endsWith(table.baseTableRow.tableName))) {
            Await.result(connect.run(table.schema.create), Duration.Inf)
          }
        }
        connect.close()
        init()
      case _ =>
        logger.error(s"init error ")
        connect.close()
    }


  }

  def init(): Unit = {
    UserStore.query(filter = Some({
      users: Users => users.name === User.Admin.name
    })).onComplete {
      case us if us.isSuccess =>
        if (us.get.size <= 0) {
          UserStore.add(User.Admin).onComplete {
            case a =>
              logger.info(s"init use success")
          }
        }

    }
  }

}*/
