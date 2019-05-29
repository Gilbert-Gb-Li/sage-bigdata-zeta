package com.haima.sage.bigdata.etl.utils

import com.haima.sage.bigdata.etl.common.model.RichMap
import com.haima.sage.bigdata.etl.common.base.Lexer
import com.haima.sage.bigdata.etl.common.model.Parser
import com.haima.sage.bigdata.etl.common.model.filter.MapRule
import com.typesafe.config.ConfigFactory

import scala.util.Try

/**
  * Created by zhhuiyan on 15/3/31.
  */
object lexerFactory extends Logger {

  /*
  *
  * 获取真正的解析器的实现
  * */
  def instance(parser: Parser[MapRule]): Option[Lexer[String, RichMap]] =
    Try {
      Class.forName(ConfigFactory.load("application.conf").getConfig("app.lexer").getString(parser.name) + "$")
    }.map(clazz => {
      val constructor = clazz.getDeclaredConstructors.head
      constructor.setAccessible(true)
      clazz.getMethod("apply", parser.getClass).invoke(constructor.newInstance(), parser).asInstanceOf[Lexer[String, RichMap]]
    }).toOption

}
