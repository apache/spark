package org.apache.spark.deploy.yarn

import org.apache.spark.SparkEnv
import org.apache.spark.SparkConf
import akka.actor.ActorSystem
import akka.actor.ActorSelection
import akka.actor.Props
import org.apache.spark.Logging
import akka.actor.Actor
import akka.remote.RemotingLifecycleEvent
import akka.remote.DisassociatedEvent
import org.apache.spark.autoscale.AutoscaleMessages._

private[spark] class AutoscaleServer(allocator: YarnAllocator, sparkConf: SparkConf, actorSystem: ActorSystem, isClientRemote : Boolean = true) extends Logging {
  
  def start() {
    var driverUrl = if (isClientRemote) {
      "akka.tcp://%s@%s:%s/user/%s".format(
      SparkEnv.driverActorSystemName,
      sparkConf.get("spark.driver.host"),
      sparkConf.get("spark.driver.port"),
      "AutoscaleClient")
    } else {
        "akka://%s/user/%s".format(
        SparkEnv.driverActorSystemName,
        "AutoscaleClient")
    }
    actorSystem.actorOf(Props(new AutoscaleServerActor(driverUrl)), name = "AutoscaleServerActor")
  }
  
  private class AutoscaleServerActor(driverUrl: String) extends Actor {

    var driver: ActorSelection = _

    override def preStart() = {
      logInfo("Listen to driver: " + driverUrl)
      driver = context.actorSelection(driverUrl)
      // Send a hello message to establish the connection, after which
      // we can monitor Lifecycle Events.
      driver ! "Hello"
      driver ! RegisterAutoscaleServer
      context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])
    }

    override def receive = {
      case x: DisassociatedEvent =>
        logInfo(s"Driver terminated or disconnected! Shutting down. $x")
        //finish(FinalApplicationStatus.SUCCEEDED)
      case RegisteredAutoscaleServer =>
        logInfo("Autoscaler registered successfully")
      case AddExecutors(count: Int) =>
        logInfo("Autoscaler : request to add " + count + "executors")
        allocator.addExecutors(count)
      case DeleteExecutors(execIds: List[String]) =>
        logInfo("Autoscaler: request to delete executor : " + execIds)
        allocator.deleteExecutors(execIds)
    }

  }

}