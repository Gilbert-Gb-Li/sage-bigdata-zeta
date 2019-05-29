package com.haima.sage.bigdata.etl.authority

import java.util.UUID

import com.haima.sage.bigdata.etl.authority.Role._


/**
  * Created by zhhuiyan on 15/5/5.
  */
case class Group(id: String = UUID.randomUUID().toString, name: String, roles: Set[Role] = Set(), describe: String)

object SYSTEM_GROUP extends Group("0", "SYSTEM_GROUP", Set(SYSTEM_ROLE), "SYSTEM_GROUP")

object ADMIN_GROUP extends Group("1", "ADMIN_GROUP", Set(ADMIN_ROLE), "ADMIN_GROUP")
