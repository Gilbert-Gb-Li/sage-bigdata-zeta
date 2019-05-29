package com.haima.sage.bigdata.etl.common.model

import com.fasterxml.jackson.core.`type`.TypeReference

/**
  * Created by zhhuiyan on 2017/2/8.
  */
trait Authorization {
  // @JsonScalaEnumeration(classOf[AuthType.AuthType])
  def authentication: Option[AuthType.AuthType] = Some(AuthType.KERBEROS)

}

class AuthTypeType extends TypeReference[AuthType.type]

object AuthType extends Enumeration {
  type AuthType = Value
  val KERBEROS, PLAIN, SSL, PLAIN_SSL, NONE = Value
}