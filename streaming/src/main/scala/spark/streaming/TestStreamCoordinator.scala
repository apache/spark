package spark.streaming

import spark.Logging

import akka.actor._
import akka.actor.Actor
import akka.actor.Actor._

sealed trait TestStreamCoordinatorMessage
case class GetStreamDetails extends TestStreamCoordinatorMessage
case class GotStreamDetails(name: String, duration: Long) extends TestStreamCoordinatorMessage 
case class TestStarted extends TestStreamCoordinatorMessage

class TestStreamCoordinator(streamDetails: Array[(String, Long)]) extends Actor with Logging {
  
  var index = 0 

  initLogging()

  logInfo("Created")

  def receive = {
    case TestStarted => {
      sender ! "OK"
    }

    case GetStreamDetails => {
      val streamDetail = if (index >= streamDetails.length) null else streamDetails(index)
      sender ! GotStreamDetails(streamDetail._1, streamDetail._2)
      index += 1
      if (streamDetail != null) {
        logInfo("Allocated " + streamDetail._1 + " (" + index + "/" + streamDetails.length + ")" )
      } 
    }
  }
  
}

