package spark.deploy

import akka.actor.{ActorRef, Props, Actor, ActorSystem, Terminated}

import spark.deploy.worker.Worker
import spark.deploy.master.Master
import spark.util.AkkaUtils
import spark.{Logging, Utils}

import scala.collection.mutable.ArrayBuffer

/**
 * Testing class that creates a Spark standalone process in-cluster (that is, running the
 * spark.deploy.master.Master and spark.deploy.worker.Workers in the same JVMs). Executors launched
 * by the Workers still run in separate JVMs. This can be used to test distributed operation and
 * fault recovery without spinning up a lot of processes.
 */
private[spark]
class LocalSparkCluster(numWorkers: Int, coresPerWorker: Int, memoryPerWorker: Int) extends Logging {
  
  val localIpAddress = Utils.localIpAddress
  
  var masterActor : ActorRef = _
  var masterActorSystem : ActorSystem = _
  var masterPort : Int = _
  var masterUrl : String = _
  
  val workerActorSystems = ArrayBuffer[ActorSystem]()
  val workerActors = ArrayBuffer[ActorRef]()
  
  def start() : String = {
    logInfo("Starting a local Spark cluster with " + numWorkers + " workers.")

    /* Start the Master */
    val (actorSystem, masterPort) = AkkaUtils.createActorSystem("sparkMaster", localIpAddress, 0)
    masterActorSystem = actorSystem
    masterUrl = "spark://" + localIpAddress + ":" + masterPort
    masterActor = masterActorSystem.actorOf(
      Props(new Master(localIpAddress, masterPort, 0)), name = "Master")

    /* Start the Slaves */
    for (workerNum <- 1 to numWorkers) {
      val (actorSystem, boundPort) = 
        AkkaUtils.createActorSystem("sparkWorker" + workerNum, localIpAddress, 0)
      workerActorSystems += actorSystem
      val actor = actorSystem.actorOf(
        Props(new Worker(localIpAddress, boundPort, 0, coresPerWorker, memoryPerWorker, masterUrl)),
              name = "Worker")
      workerActors += actor
    }

    return masterUrl
  }

  def stop() {
    logInfo("Shutting down local Spark cluster.")
    // Stop the workers before the master so they don't get upset that it disconnected
    workerActorSystems.foreach(_.shutdown())
    workerActorSystems.foreach(_.awaitTermination())
    masterActorSystem.shutdown()
    masterActorSystem.awaitTermination()
  }
}
