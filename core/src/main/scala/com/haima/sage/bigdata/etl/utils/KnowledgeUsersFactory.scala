package com.haima.sage.bigdata.etl.utils

import akka.actor.{ActorRef, ActorSystem, Props}
import com.haima.sage.bigdata.etl.server.knowledge.KnowledgeUserActor

import scala.collection.mutable

/**
  * Created by liyju on 2018/4/16.
  */
object KnowledgeUsersFactory extends Logger {
  private val users: mutable.Map[String, ActorRef] = mutable.Map[String, ActorRef]()

  def createKnowledgeUser(name: String)(implicit system: ActorSystem): ActorRef = system.actorOf(Props.create(classOf[KnowledgeUserActor], name, "load"))

  def get(name: String)(implicit system: ActorSystem): ActorRef = this.synchronized(users.getOrElseUpdate(name, createKnowledgeUser(name)(system)))

  def close(name: String): Unit = this.synchronized(users.remove(name))

}
