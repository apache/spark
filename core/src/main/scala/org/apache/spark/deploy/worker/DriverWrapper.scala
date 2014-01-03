package org.apache.spark.deploy.worker

import akka.actor._

import org.apache.spark.util.{AkkaUtils, Utils}

/**
 * Utility object for launching driver programs such that they share fate with the Worker process.
 */
object DriverWrapper {
  def main(args: Array[String]) {
    args.toList match {
      case workerUrl :: mainClass :: extraArgs =>
        val (actorSystem, _) = AkkaUtils.createActorSystem("Driver",
          Utils.localHostName(), 0)
        actorSystem.actorOf(Props(classOf[WorkerWatcher], workerUrl), name = "workerWatcher")

        // Delegate to supplied main class
        val clazz = Class.forName(args(1))
        val mainMethod = clazz.getMethod("main", classOf[Array[String]])
        mainMethod.invoke(null, extraArgs.toArray[String])

        actorSystem.awaitTermination()

      case _ =>
        System.err.println("Usage: DriverWrapper <workerUrl> <driverMainClass> [options]")
        System.exit(-1)
    }
  }
}