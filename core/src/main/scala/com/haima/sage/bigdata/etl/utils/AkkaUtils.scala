package com.haima.sage.bigdata.etl.utils

import akka.actor.{ActorPath, ActorRef, ActorSystem, Address, ExtendedActorSystem, Extension, ExtensionKey}


/** [[akka.actor.ActorSystem]] [[Extension]] used to obtain the [[Address]] on which the
  * given ActorSystem is listening.
  *
  * @param system
  */
class AkkaAddressExtensionImplementation(system: ExtendedActorSystem) extends Extension {
  def address: Address = system.provider.getDefaultAddress
}

object AkkaAddressExtension extends ExtensionKey[AkkaAddressExtensionImplementation] {}

/**
  * Created by zhhuiyan on 2017/5/15.
  */
object AkkaUtils extends ExtensionKey[AkkaAddressExtensionImplementation] {


  def getPath(ref: ActorRef)(implicit system: ActorSystem): ActorPath = {

    ActorPath.fromString(AkkaAddressExtension(system).address.toString + ref.path.toStringWithoutAddress)
  }

   def getPathString(ref: ActorRef)(implicit system: ActorSystem) :String = {
     AkkaAddressExtension(system).address.toString + ref.path.toStringWithoutAddress
   }
}
