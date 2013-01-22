package spark.deploy

import akka.actor.{ActorRef, Props, Actor, ActorSystem, Terminated}

import spark.deploy.worker.Worker
import spark.deploy.master.Master
import spark.util.AkkaUtils
import spark.{Logging, Utils}

import scala.collection.mutable.ArrayBuffer

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

    /* Start the Workers */
    for (workerNum <- 1 to numWorkers) {
      /* We can pretend to test distributed stuff by giving the workers distinct hostnames.
         All of 127/8 should be a loopback, we use 127.100.*.* in hopes that it is
         sufficiently distinctive. */
      val workerIpAddress = "127.100.0." + (workerNum % 256)
      val (actorSystem, boundPort) = 
        AkkaUtils.createActorSystem("sparkWorker" + workerNum, workerIpAddress, 0)
      workerActorSystems += actorSystem
      workerActors += actorSystem.actorOf(
        Props(new Worker(workerIpAddress, boundPort, 0, coresPerWorker, memoryPerWorker, masterUrl)),
        name = "Worker")
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
