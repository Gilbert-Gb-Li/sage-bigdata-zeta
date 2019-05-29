/*
package com.haima.sage.bigdata.etl.authority

import java.util.UUID

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility
import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.annotation.PropertyAccessor
import com.fasterxml.jackson.databind.{ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.haima.sage.bigdata.etl.store.slick.{BaseStore, TableID, WithID}
import slick.driver.DerbyDriver.api._
import slick.lifted.{TableQuery, Tag}

import scala.concurrent.Future

/**
  * Created by zhhuiyan on 2016/9/30.
  */
case class SecretKeyPair(id: String = UUID.randomUUID().toString,
                         key: String,
                         sign: String,
                         expire: Long,
                         disable: Boolean,
                         userId: String) extends WithID[String] {
}


class SecretKeyPairs(tag: Tag) extends Table[SecretKeyPair](tag, "user") with TableID[String] {

  val id: Rep[String] = column[String]("id", O.PrimaryKey)

  def key: Rep[String] = column[String]("key")

  def sign: Rep[String] = column[String]("sign")

  def expire: Rep[Long] = column[Long]("expire")

  def disable: Rep[Boolean] = column[Boolean]("disable")

  def userId: Rep[String] = column[String]("user")

  def user = foreignKey("US_FK", userId, UserStore.table)(_.id, onUpdate = ForeignKeyAction.Restrict, onDelete = ForeignKeyAction.Cascade)

  def * = (id, key, sign, expire, disable, userId).shaped <> (SecretKeyPair.tupled, SecretKeyPair.unapply)
}


object SecretKeyPairStore extends BaseStore[SecretKeyPairs, SecretKeyPair, String] {

  import  context.dispatcher

  def isExist(key: String, sign: String): Future[Boolean] = {

    val connect = db
    val future = connect.run((table.filter(data => data.key === key && data.sign === sign && !data.disable).size > 0).result)
    future.onComplete(_ => connect.close())
    future
  }


  override implicit def equals(wrap: Rep[String], real: String): Rep[Boolean] = {
    wrap === real
  }


  override def table: TableQuery[SecretKeyPairs] = TableQuery[SecretKeyPairs]

  def getUser(id: String): Future[Option[LoginUser]] = {
    val connect = db
    val future: Future[Option[LoginUser]] = connect.run(
      table.filter(data => equals(data.id, id)).flatMap(_.user).result.headOption)
    future.onComplete {
      data =>
        connect.close()
    }

    future
  }
}*/
