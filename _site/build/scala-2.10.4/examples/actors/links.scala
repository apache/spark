package examples.actors

import scala.actors.{Actor, Exit}
import scala.actors.Actor._

object links extends App {

  case object Stop

  actor {
    val start = link(p(2))
    start ! Stop
  }

  def p(n: Int): Actor =
    if (n == 0) top1()
    else top(p(n-1), n)

  def top(a: Actor, n: Int): Actor = actor {
    println("starting actor " + n + " (" + self + ")")
    self.trapExit = true
    link(a)
    loop {
      receive {
        case ex @ Exit(from, reason) =>
          println("Actor " + n + " received " + ex)
          exit('finished)
        case any => {
          println("Actor " + n + " received " + any)
          a ! any
        }
      }
    }
  }

  def top1(): Actor = actor {
    println("starting last actor"  + " (" + self + ")")
    receive {
      case Stop =>
        println("Last actor now exiting")
        exit('abnormal)
      case any =>
        println("Last actor received " + any)
        top1()
    }
  }
}
