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
  
  private val localHostname = Utils.localHostName()
  private val masterActorSystems = ArrayBuffer[ActorSystem]()
  private val workerActorSystems = ArrayBuffer[ActorSystem]()
  
  def start(): String = {
    logInfo("Starting a local Spark cluster with " + numWorkers + " workers.")

    /* Start the Master */
    val (masterSystem, masterPort) = Master.startSystemAndActor(localHostname, 0, 0)
    masterActorSystems += masterSystem
    val masterUrl = "spark://" + localHostname + ":" + masterPort

    /* Start the Workers */
    for (workerNum <- 1 to numWorkers) {
      val (workerSystem, _) = Worker.startSystemAndActor(localHostname, 0, 0, coresPerWorker,
        memoryPerWorker, masterUrl, null, Some(workerNum))
      workerActorSystems += workerSystem
    }

    return masterUrl
  }

  def stop() {
    logInfo("Shutting down local Spark cluster.")
    // Stop the workers before the master so they don't get upset that it disconnected
    workerActorSystems.foreach(_.shutdown())
    workerActorSystems.foreach(_.awaitTermination())
    masterActorSystems.foreach(_.shutdown())
    masterActorSystems.foreach(_.awaitTermination())
  }
}
