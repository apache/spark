package org.apache.spark.deploy.worker


import akka.testkit.TestActorRef
import org.scalatest.FunSuite
import akka.remote.DisassociatedEvent
import akka.actor.{ActorSystem, AddressFromURIString, Props}

class WorkerWatcherSuite extends FunSuite {
  test("WorkerWatcher shuts down on valid disassociation") {
    val actorSystem = ActorSystem("test")
    val targetWorkerUrl = "akka://1.2.3.4/user/Worker"
    val targetWorkerAddress = AddressFromURIString(targetWorkerUrl)
    val actorRef = TestActorRef[WorkerWatcher](Props(classOf[WorkerWatcher], targetWorkerUrl))(actorSystem)
    val workerWatcher = actorRef.underlyingActor
    workerWatcher.setTesting(testing = true)
    actorRef.underlyingActor.receive(new DisassociatedEvent(null, targetWorkerAddress, false))
    assert(actorRef.underlyingActor.isShutDown)
  }

  test("WorkerWatcher stays alive on invalid disassociation") {
    val actorSystem = ActorSystem("test")
    val targetWorkerUrl = "akka://1.2.3.4/user/Worker"
    val otherAkkaURL = "akka://4.3.2.1/user/OtherActor"
    val otherAkkaAddress = AddressFromURIString(otherAkkaURL)
    val actorRef = TestActorRef[WorkerWatcher](Props(classOf[WorkerWatcher], targetWorkerUrl))(actorSystem)
    val workerWatcher = actorRef.underlyingActor
    workerWatcher.setTesting(testing = true)
    actorRef.underlyingActor.receive(new DisassociatedEvent(null, otherAkkaAddress, false))
    assert(!actorRef.underlyingActor.isShutDown)
  }
}