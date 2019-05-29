package com.haima.sage.bigdata.analyzer.utils

import akka.actor.ActorSystem
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.runtime.akka.AkkaUtils

import scala.collection.mutable

object ActorSystemFactory {

  private val systems: mutable.Map[String, ActorSystem] = mutable.Map[String, ActorSystem]()


  def get(name: String): ActorSystem = {
    systems.getOrElseUpdate(name, createActorSystem(name))
  }

  def get(name: String, host: String = "127.0.0.1", port: Int = 0): ActorSystem = {

    val configs = ConfigFactory.parseString(
      s"""akka.remote.netty.tcp.hostname = $host
          akka.remote.netty.tcp.port = $port""".stripMargin).withFallback(config())
    systems.getOrElseUpdate(name, ActorSystem.create(name, configs))
  }

  def close(name: String): Unit = {
    systems.remove(name).foreach(_.terminate())
  }


  def config(): Config = {
    ConfigFactory.parseString(
      """akka {
        |  actor {
        |    provider = "akka.remote.RemoteActorRefProvider"
        |    serializers {
        |        proto = "akka.remote.serialization.ProtobufSerializer"
        |    }
        |    serialization-bindings {
        |    }
        |  }
        |
        |  remote {
        |    maximum-payload-bytes = 30000000 bytes
        |    netty.tcp {
        |      message-frame-size =  30000000b
        |      send-buffer-size =  30000000b
        |      receive-buffer-size =  30000000b
        |      maximum-frame-size = 30000000b
        |    }
        |    prune-quarantine-marker-after = 5 d
        |    quarantine-after-silence = 6 d
        |    retry-gate-closed-for = 10000
        |    retry-window=60 s
        |    maximum-retries-in-window=10
        |    gate-invalid-addresses-for=6 d
        |    quarantine-systems-for=6 d
        |    system-message-buffer-size=100000000
        |    system-message-ack-piggyback-timeout=20 s
        |  }
        |}
      """.stripMargin).withFallback(AkkaUtils.getDefaultAkkaConfig)
  }

  /**
    * Creates an actor system with default configurations for Receiver actor.
    *
    * @return Actor System instance with default configurations
    */
  private def createActorSystem(name: String): ActorSystem = {
    ActorSystem.create(name, config())
  }
}
