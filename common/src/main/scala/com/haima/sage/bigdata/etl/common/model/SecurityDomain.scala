package com.haima.sage.bigdata.etl.common.model

import java.util.UUID

/**
  * Created: 2016-05-24 13:16.
  * Author:zhhuiyan
  * Created: 2016-05-24 13:16.
  *
  *
  */
case class SecurityDomain(id: String = UUID.randomUUID().toString,
                          name: String,
                          orderIndex: Int = 0)