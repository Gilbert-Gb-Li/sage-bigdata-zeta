package com.haima.sage.bigdata.etl.authority

import java.util.UUID

import com.haima.sage.bigdata.etl.authority.Authorization._
import com.haima.sage.bigdata.etl.authority.Resource._

/**
  * Created by zhhuiyan on 15/5/5.
  */
sealed case class Role( id: String = UUID.randomUUID().toString, name: String, powers: List[(Resource, Set[Authorization])] = Nil, describe: String)

object Role {
  val DSM = new Role(name = "DSM",
    powers = List((DATA_SOURCE, Set(SHOW, CREATE, DELETE, UPDATE, UPLOAD, DOWNLOAD))),
    describe = "DATASOURCE MANAGE")

  val PRM = new Role(name = "PRM",
    powers = List((PARSE_RULE, Set(SHOW, CREATE, DELETE, UPDATE, UPLOAD, DOWNLOAD))),
    describe = "PARSERULE MANAGE")

  val DM = new Role(name = "DM",
    powers = List((PARSE_RULE, Set(SHOW, CREATE, DELETE, UPDATE, UPLOAD, DOWNLOAD))),
    describe = "DICTIONARY MANAGE")

  val AM = new Role(name = "AM",
    powers = List((ASSET, Set(SHOW, CREATE, DELETE, UPDATE, UPLOAD, DOWNLOAD))),
    describe = "ASSET MANAGE")

  val ATM = new Role(name = "ATM",
    powers = List((ASSET_TYPE, Set(SHOW, CREATE, DELETE, UPDATE, UPLOAD, DOWNLOAD))),
    describe = "ASSET_TYPE MANAGE")

  val WM = new Role(name = "WM",
    powers = List((WRITER, Set(SHOW, CREATE, DELETE, UPDATE, UPLOAD, DOWNLOAD))),
    describe = "WRITER MANAGE")


  val UM = new Role(name = "UM",
    powers = List((USER, Set(SHOW, CREATE, DELETE, UPDATE))),
    describe = "USER MANAGE")

  val RM = new Role(name = "RM",
    powers = List((ROLE, Set(SHOW, CREATE, DELETE, UPDATE))),
    describe = "ROLE MANAGE")

  val GM = new Role(name = "GM",
    powers = List((GROUP, Set(SHOW, CREATE, DELETE, UPDATE))),
    describe = "GROUP MANAGE")


  val ADMIN_ROLE = new Role(name = "ADMIN", powers = UM.powers ::: GM.powers ::: RM.powers, describe = "ADMIN")

  val SYSTEM_ROLE = new Role(name = "ADMIN", powers = DSM.powers ::: PRM.powers ::: DM.powers ::: AM.powers ::: ATM.powers ::: WM.powers, describe = "SYSTEM")

  val SUPER_ROLE = new Role(name = "ADMIN", powers = ADMIN_ROLE.powers ::: SYSTEM_ROLE.powers, describe = "SUPER ROLE")
}


