package org.apache.spark.autoscale

import org.apache.spark.SparkEnv
import akka.actor.ActorRef
import akka.actor.Actor
import akka.actor.Props
import akka.actor.ActorSelection
import akka.actor.ActorSystem
import org.apache.spark.Logging
import org.apache.spark.autoscale.AutoscaleMessages._

class AutoscaleClient(actorSystem: ActorSystem) extends Logging {
  var server : ActorRef = _
  actorSystem.actorOf(Props(new AutoscaleClientActor()), "AutoscaleClient")
  
  private class AutoscaleClientActor extends Actor {
    override def preStart = {
      logInfo("Starting Autoscale Client Actor")
    }
    override def receive = {
      case RegisterAutoscaleServer =>
        logInfo("Registering Autoscale Server Actor")
         server = sender
         sender ! true
    }
  }
  def addExecutors(count: Int) {
    if (server != null)
      server ! AddExecutors(count)
    else
      logInfo("Autoscale Server not registered")
  }
  def deleteExecutors(execIds: List[String]) {
    if (server!= null)
      server ! DeleteExecutors(execIds)
    else
      logInfo("Autoscale Server not registered")
  }
}

