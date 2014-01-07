package org.apache.spark.deploy.worker

import akka.actor.{Actor, Address, AddressFromURIString}
import akka.remote.{AssociatedEvent, AssociationErrorEvent, AssociationEvent, DisassociatedEvent, RemotingLifecycleEvent}

import org.apache.spark.Logging
import org.apache.spark.deploy.DeployMessages.SendHeartbeat

/**
 * Actor which connects to a worker process and terminates the JVM if the connection is severed.
 * Provides fate sharing between a worker and its associated child processes.
 */
private[spark] class WorkerWatcher(workerUrl: String) extends Actor
    with Logging {
  override def preStart() {
    context.system.eventStream.subscribe(self, classOf[RemotingLifecycleEvent])

    logInfo(s"Connecting to worker $workerUrl")
    val worker = context.actorSelection(workerUrl)
    worker ! SendHeartbeat // need to send a message here to initiate connection
  }

  // Used to avoid shutting down JVM during tests
  private[deploy] var isShutDown = false
  private[deploy] def setTesting(testing: Boolean) = isTesting = testing
  private var isTesting = false

  // Lets us filter events only from the worker's actor system
  private val expectedHostPort = AddressFromURIString(workerUrl).hostPort
  private def isWorker(address: Address) = address.hostPort == expectedHostPort

  def exitNonZero() = if (isTesting) isShutDown = true else System.exit(-1)

  override def receive = {
    case AssociatedEvent(localAddress, remoteAddress, inbound) if isWorker(remoteAddress) =>
      logInfo(s"Successfully connected to $workerUrl")

    case AssociationErrorEvent(cause, localAddress, remoteAddress, inbound)
        if isWorker(remoteAddress) =>
      // These logs may not be seen if the worker (and associated pipe) has died
      logError(s"Could not initialize connection to worker $workerUrl. Exiting.")
      logError(s"Error was: $cause")
      exitNonZero()

    case DisassociatedEvent(localAddress, remoteAddress, inbound) if isWorker(remoteAddress) =>
      // This log message will never be seen
      logError(s"Lost connection to worker actor $workerUrl. Exiting.")
      exitNonZero()

    case e: AssociationEvent =>
      // pass through association events relating to other remote actor systems

    case e => logWarning(s"Received unexpected actor system event: $e")
  }
}