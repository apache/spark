package spark.deploy

import akka.actor.{ActorRef, Props, Actor, ActorSystem, Terminated}

import spark.deploy.worker.Worker
import spark.deploy.master.Master
import spark.util.AkkaUtils
import spark.{Logging, Utils}

import scala.collection.mutable.ArrayBuffer

object DeployUtils extends Logging {
  
  /* Starts a local standalone Spark cluster with a specified number of slaves */
  def startLocalSparkCluster(numSlaves : Int, coresPerSlave : Int, 
    memoryPerSlave : Int) : String = {
    
    logInfo("Starting a local Spark cluster with " + numSlaves + " slaves.")
    
    val threadPool = Utils.newDaemonFixedThreadPool(numSlaves + 1)
    val localIpAddress = Utils.localIpAddress
    val workers = ArrayBuffer[ActorRef]()
    
    /* Start the Master */
    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("sparkMaster", localIpAddress, 0)
    val masterUrl = "spark://" + localIpAddress + ":" + boundPort
    threadPool.execute(new Runnable {
      def run() {
        val actor = actorSystem.actorOf(
          Props(new Master(localIpAddress, boundPort, 8080)), name = "Master")
        actorSystem.awaitTermination()
      }
    })
    
    /* Start the Slaves */
    (1 to numSlaves + 1).foreach { slaveNum =>
      val (actorSystem, boundPort) = 
        AkkaUtils.createActorSystem("sparkWorker" + slaveNum, localIpAddress, 0)
      threadPool.execute(new Runnable {
        def run() {  
          val actor = actorSystem.actorOf(
            Props(new Worker(localIpAddress, boundPort, 8080 + slaveNum, coresPerSlave, memoryPerSlave, masterUrl)),
            name = "Worker")
          workers += actor
          actorSystem.awaitTermination()
        }
      })
    }
    
    return masterUrl
  }
  
}