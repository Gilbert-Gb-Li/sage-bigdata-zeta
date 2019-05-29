package com.haima.sage.bigdata.etl.common.model

import java.sql.Timestamp
import java.util.{Date, UUID}

/**
  * Created: 2016-05-26 15:09.
  * Author:zhhuiyan
  * Created: 2016-05-26 15:09.
  *
  *
  */
case class Dictionary(id: String = UUID.randomUUID().toString,
                      name: String,
                      key: String,
                      properties: List[DictionaryProperties] = List(),
                      lasttime: Timestamp = new Timestamp(System.currentTimeMillis()),
                      createtime: Timestamp = new Timestamp(System.currentTimeMillis())
                     )

case class DictionaryProperties(key: String,
                                value: String,
                                vtype: String,
                                lasttime: Timestamp = new Timestamp(System.currentTimeMillis()),
                                createtime: Timestamp = new Timestamp(System.currentTimeMillis())
                               )