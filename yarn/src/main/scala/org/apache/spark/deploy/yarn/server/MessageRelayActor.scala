package org.apache.spark.deploy.yarn.server

import akka.actor.{Actor, ActorSelection, Props, ActorRef}
import akka.pattern.AskSupport
import akka.util.Timeout
import org.apache.spark.SparkContext
import org.apache.spark.deploy.yarn._
import org.apache.spark.deploy.yarn.server.ApplicationContext._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try


/**
 *
 * MessageRelayActor is a Server side Actor that created by Spark Job
 * that relay the message send from spark job to spark Application.
 *
 * In particular, this server side actor to send message to clientListener.
 *
 *
 * @param clientListener  -- Yarn Client Side Actor to receive messages
 * @param appName -- spark app name
 * @param sc -- sparkContext
 */
class MessageRelayActor(val clientListener: ActorRef,
                        appName: String,
                        sc: SparkContext) extends Actor with AskSupport{


  //relay the message to clientListener
  def stopSparkJob(name: String, args: JobArg*) = {
    if (name == appName) {
      sc.stop()
    } else {
      val ignoreMsg: String = s"stopJob command for application $name is ignored, " +
        s"as the current application is $appName"
      println(ignoreMsg)

      clientListener ! ignoreMsg
    }
  }

  def receive: Actor.Receive = {
    case Pong =>
      println("clientListener = " + sender)
      println("clientListener response Pong")

    case x @ AppInit => clientListener ! x
    case x @ AppStart => clientListener ! x
    case x @ AppEnd => clientListener ! x
    case x @ AppKilled => clientListener ! x
    case x @ AppProgress => clientListener ! x
    case x @ AppFailed => clientListener ! x


    //receive message from clientListener
    case StartSparkJob(name, args) =>
      println(" get job start job message")
    case StopSparkJob(name) =>
      stopSparkJob(name)

    case StopSparkJob(name, args) =>
      stopSparkJob(name, args)

    //catch all messages
    case x: Any =>
      Console.err.println(s"get message: $x")
      clientListener ! x
    case x @ _ => clientListener ! x

  }

}


object AkkaChannelUtils {

  def resolveClientActor(clientListener: ActorSelection, handshakeTimeout: Timeout): Try[ActorRef] = {
    println("perform handshake between client application and spark job ")
    val t = Try {
      implicit val timeout = handshakeTimeout
      val f = clientListener.resolveOne()
      Await.result(f, handshakeTimeout.duration)
    }

    import scala.util.{ Failure, Success }
    t match {
      case Success(a) => Console.err.println("actor resolved")
      case Failure(ex) => ex.printStackTrace(Console.err)
    }

    t
  }

  def createRelayMessenger(appCtx: ApplicationContext): ChannelMessenger = {
    val timeout = appCtx.conf.get(SPARK_YARN_APP_HANDSHAKE_TIMEOUT, "5").toInt
    val handShakeTimeout = new Timeout(timeout seconds)

    def resolved: Option[ActorRef] = {
      findClientSideCommunicator(appCtx.sparkCtx).map { clientListener =>
        resolveClientActor(clientListener, handShakeTimeout).toOption
      }.flatten
    }

    val messenger :Option[ActorRef] = resolved.map { actor =>
      appCtx.sparkCtx.env.actorSystem.actorOf(Props(new MessageRelayActor(actor,
        appCtx.appName, appCtx.sparkCtx)), "akka-replay-messenger")
    }

    if (!messenger.isDefined) {
      Console.err.println("===============================================")
      Console.err.println(" Unable to setup Akka Relay Messenger ")
      Console.err.println("===============================================")
    }

    //hand-shake
    messenger.foreach(_ ! Ping)

    ActorMessenger(messenger)
  }

  def findClientSideCommunicator(sc: SparkContext): Option[ActorSelection] = {

    sc.getConf.getOption("app.spark.yarn.client.listener.uri").map { uri =>
      Console.err.println(s"**************************************************")
      Console.err.println(s" client listener uri = $uri")
      Console.err.println(s"**************************************************")
      val actorSystem = sc.env.actorSystem
      actorSystem.actorSelection(uri)

    }
  }
}
