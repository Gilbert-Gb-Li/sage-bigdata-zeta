package com.haima.sage.bigdata.etl.store

import java.util.UUID

import com.haima.sage.bigdata.etl.authority.{ADMIN_GROUP, Group, Role, SYSTEM_GROUP}

/**
  * Created by zhhuiyan on 15/5/5.
  */

sealed case class LoginUser(id: String = UUID.randomUUID().toString, name: String, email: String,
                            roles: List[Role] = Nil, groups: List[Group] = Nil,
                            describe: String, password: String)
  extends Serializable // with WithID[String]

object User {
  val Admin = LoginUser(name = "admin", email = "admin@admin", groups = List(SYSTEM_GROUP, ADMIN_GROUP), describe = "admin", password = "123456")
}

/*


class Users(tag: Tag) extends Table[LoginUser](tag, "user") with TableID[String] with Mapper {
  implicit def groupsToString: JdbcType[List[Group]] with BaseTypedType[List[Group]] =
    MappedColumnType.base[List[Group], String](mapper.writeValueAsString(_),
      mapper.readValue[List[Group]])

  implicit def rolesToString: JdbcType[List[Role]] with BaseTypedType[List[Role]] =
    MappedColumnType.base[List[Role], String](mapper.writeValueAsString(_),
      mapper.readValue[List[Role]])

  val id: Rep[String] = column[String]("id", O.PrimaryKey)

  def name: Rep[String] = column[String]("name")

  def email: Rep[String] = column[String]("email")

  def roles: Rep[List[Role]] = column[List[Role]]("roles")

  def groups: Rep[List[Group]] = column[List[Group]]("groups", O.Length(2000))

  def describe: Rep[String] = column[String]("describe")

  def password: Rep[String] = column[String]("password")

  def * : ProvenShape[LoginUser] = (id, name, email, roles, groups, describe, password).shaped <> (LoginUser.tupled, LoginUser.unapply)
}


object UserStore extends BaseStore[Users, LoginUser, String] {

  override implicit def equals(wrap: Rep[String], real: String): Rep[Boolean] = {
    wrap === real
  }

  def query(name: String, password: String): Future[Option[LoginUser]] = {
    val connect = db
    val future = connect.run(
      table.filter(user => (user.email === name || user.name === name) && user.password == password).result.headOption)
    future.onComplete(_ => connect.close())
    future

  }

  override def table: TableQuery[Users] = TableQuery[Users]

}*/
