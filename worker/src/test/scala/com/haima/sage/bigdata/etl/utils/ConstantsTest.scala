package com.haima.sage.bigdata.etl.utils

import com.typesafe.config.{ConfigFactory, ConfigIncluder, ConfigParseOptions, ConfigResolveOptions}
import org.junit.Test

/**
  * Created by zhhuiyan on 2017/4/20.
  */
class ConstantsTest {

  @Test
  def test(): Unit = {
    val app = ConfigFactory.load("application.conf", ConfigParseOptions.defaults(),
      ConfigResolveOptions.noSystem())
    val worker = ConfigFactory.parseResourcesAnySyntax("worker.conf")
    println(app.getString("app.store.driver.url"))
    println(worker.getString("app.store.driver.url"))
    println(app.withFallback(worker).getString("app.store.driver.url"))
    println(worker.withFallback(app).getString("app.store.driver.url"))

    println(app.getString("akka.remote.netty.tcp.maximum-frame-size"))
  //  println(worker.getString("akka.remote.netty.tcp.maximum-frame-size"))


    println(worker.withFallback(app).getString("akka.remote.netty.tcp.maximum-frame-size"))
    println(worker.withoutPath("akka.remote.netty.tcp.maximum-frame-size").withFallback(app).getString("app.store.driver.url"))


  }
}
