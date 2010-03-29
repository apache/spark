package ubiquifs

import scala.actors.Actor
import scala.actors.Actor._
import scala.actors.remote.RemoteActor
import scala.actors.remote.RemoteActor._
import scala.actors.remote.Node
import scala.collection.mutable.{ArrayBuffer, Map, Set}

class Master(port: Int) extends Actor {
  case class SlaveInfo(host: String, port: Int)

  val files = Set[String]()
  val slaves = new ArrayBuffer[SlaveInfo]()

  def act() {
    alive(port)
    register('UbiquiFS, self)
    println("Created UbiquiFS Master on port " + port)

    loop {
      react {
        case RegisterSlave(host, port) =>
          slaves += SlaveInfo(host, port)
          sender ! RegisterSucceeded()

        case Create(path) =>
          if (files.contains(path)) {
            sender ! CreateFailed("File already exists")
          } else if (slaves.isEmpty) {
            sender ! CreateFailed("No slaves registered")
          } else {
            files += path
            sender ! CreateSucceeded(slaves(0).host, slaves(0).port)
          }
            
        case m: Any =>
          println("Unknown message: " + m)
      }
    }
  }
}

object MasterMain {
  def main(args: Array[String]) {
    val port = args(0).toInt
    new Master(port).start()
  }
}
