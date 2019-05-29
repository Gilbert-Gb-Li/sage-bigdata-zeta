/*
package com.haima.sage.bigdata.etl.store.slick

import com.haima.sage.bigdata.etl.common.Constants
import com.typesafe.config.ConfigFactory
import slick.driver.DerbyDriver.api._
import slick.lifted.{Rep, TableQuery}

import  context.dispatcher
import scala.concurrent.Future

/**
  * Created by zhhuiyan on 15/8/21.
  */
abstract class BaseStore[Tables <: Table[Entry] with TableID[ID], Entry <: WithID[ID], ID] extends AutoCloseable {

 //url = "jdbc:derby:db/derby-n;create=true"

  def db = Database.forConfig("store.driver",
    ConfigFactory.parseString(s"""store.driver.url="jdbc:derby:${Constants.storePath}derby-user;create=true"""")
      .withFallback(
    Constants.MASTER))

  def table: TableQuery[Tables]


  def add(entry: Entry): Future[Int] = {
    val connect = db
    val future = connect.run(table += entry)
    future.onComplete(data => connect.close())
    future
  }

  def update(entry: Entry): Future[Int] = {
    val connect = db
    val future = connect.run(table.filter(data => equals(data.id, entry.id)).update(entry))
    future.onComplete(data => connect.close())
    future

  }


  def delete(filter: Option[Tables => Rep[Boolean]] = None): Future[Int] = {
    val connect = db
    val future = connect.run(statement(filter).delete)
    future.onComplete(_ => connect.close())
    future
  }

  def delete(id: ID): Future[Int] = {
    val connect = db
    val future = connect.run(table.filter(data => equals(data.id, id)).delete)
    future.onComplete {
      data =>
        connect.close()
    }

    future
  }

  def query(id: ID): Future[Option[Entry]] = {

    val connect = db
    val future = connect.run(
      table.filter(data => equals(data.id, id)).result.headOption)
    future.onComplete {
      data =>
        connect.close()
    }

    future
  }

  private def statement(filter: Option[Tables => Rep[Boolean]] = None) = {
    filter match {
      case None =>
        table
      case Some(f) =>
        table.filter(data => f(data))
    }
  }

  def query(filter: Option[Tables => Rep[Boolean]] = None): Future[Seq[Entry]] = {
    val connect = db
    val future = connect.run(statement(filter).result)

    future.onComplete {
      data =>
        connect.close()
    }

    future
  }


  private def count(filter: Option[Tables => Rep[Boolean]] = None): Future[Int] = {
    val connect = db
    val future = connect.run(statement(filter).length.result)
    future.onComplete {
      data =>
        connect.close()
    }

    future

  }

  def list(offset: Int = 0, limit: Int = 10, orders: String)(filter: Option[Tables => Rep[Boolean]] = None) = {
    import  context.dispatcher

    val connect = db
    val future = {


      var query = statement(filter)

      orders.split("\\|").foreach {
        data =>

          query = query.sortBy {
            tb =>
              val (asc, column) = data.charAt(0) match {
                case '-' =>
                  (true, tb.column[String](data.substring(1, data.length)))
                case '+' =>
                  (false, tb.column[String](data.substring(1, data.length)))
                case _ =>
                  (false, tb.column[String](data))
              }
              if (asc) {
                column.asc
              } else {
                column.desc
              }
          }

      }

      query = query.drop(offset).take(limit)
      val totalRows = count(filter)
      val result = connect.run(query.result)
      result flatMap (datas => totalRows map (rows => (datas, rows)))
    }
    future.onComplete {
      data =>
        connect.close()
    }

    future
  }

  def equals(wrap: Rep[ID], real: ID): Rep[Boolean]

  override def close(): Unit = {
    db.shutdown
  }
}

trait WithID[T] {
  val id: T
}

trait TableID[T] {
  val id: Rep[T]
}
*/
