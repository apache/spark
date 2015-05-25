package org.apache.spark.deploy.yarn.server

import akka.actor.ActorRef
import org.apache.spark.SparkContext
import org.apache.spark.deploy.yarn.server.ApplicationContext._
import org.apache.spark.deploy.yarn.server.ChannelProtocols._

import scala.language.postfixOps

/**
 * This class will be run inside the Yarn Cluster
 */


object ChannelProtocols {
  val AKKA = "akka"
  val NETTY = "netty"
}


sealed trait ChannelMessenger {
  val protocol: String
  val messenger: Option[Any]

  def sendMessage(sc: SparkContext, message: Any)
}

case class ActorMessenger(messenger:Option[ActorRef]) extends ChannelMessenger {
  val protocol = ChannelProtocols.AKKA

  def sendMessage(sc: SparkContext, message: Any)   = {
    implicit val actorSystem = sc.env.actorSystem
    messenger.foreach(_ ! message)
  }

}
case class NettyMessenger(messenger:Option[Any]) extends ChannelMessenger {
  val protocol = ChannelProtocols.NETTY
  def sendMessage(sc: SparkContext, message: Any) = ???

}

object CommunicationHelper {

  private[server] def stopRelayMessenger(sparkCtx: Option[SparkContext],
    channelMessenger: ChannelMessenger) {
    sparkCtx.map { sc =>
      channelMessenger.protocol match {
        case AKKA =>
          channelMessenger.messenger.asInstanceOf[Option[ActorRef]].map(sc.env.actorSystem.stop)
        case NETTY =>
        case _  =>
      }
    }
  }

  private[server] def createRelayMessenger(appCtx: ApplicationContext): ChannelMessenger = {
    val protocol = appCtx.conf.get(SPARK_APP_CHANNEL_PROTOCOL, AKKA)
    protocol match {
      case AKKA =>  AkkaChannelUtils.createRelayMessenger(appCtx)
      case NETTY => NettyMessenger(None)
      case _ => ActorMessenger(None)
    }

  }

}
