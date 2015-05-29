package examples.actors

import scala.actors.Actor
import scala.actors.Actor._

case object Ping
case object Pong
case object Stop

/**
 * Ping pong example.
 *
 * @author  Philipp Haller
 * @version 1.1
 */
object pingpong extends App {
  val pong = new Pong
  val ping = new Ping(100000, pong)
  ping.start
  pong.start
}

class Ping(count: Int, pong: Actor) extends Actor {
  def act() {
    var pingsLeft = count - 1
    pong ! Ping
    loop {
      react {
        case Pong =>
          if (pingsLeft % 1000 == 0)
            println("Ping: pong")
          if (pingsLeft > 0) {
            pong ! Ping
            pingsLeft -= 1
          } else {
            println("Ping: stop")
            pong ! Stop
            exit()
          }
      }
    }
  }
}

class Pong extends Actor {
  def act() {
    var pongCount = 0
    loop {
      react {
        case Ping =>
          if (pongCount % 1000 == 0)
            println("Pong: ping "+pongCount)
          sender ! Pong
          pongCount += 1
        case Stop =>
          println("Pong: stop")
          exit()
      }
    }
  }
}
