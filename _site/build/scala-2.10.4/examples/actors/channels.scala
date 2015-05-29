package examples.actors

import scala.actors._
import scala.actors.Actor._

object channels extends App {
  case class Msg(ch1: Channel[Int], ch2: Channel[String])

  val a = actor {
    val Ch1 = new Channel[Int]
    val Ch2 = new Channel[String]

    b ! Msg(Ch1, Ch2)

    val ICh1 = Ch1.asInstanceOf[InputChannel[Int]]
    val ICh2 = Ch2.asInstanceOf[InputChannel[String]]

    react {
      case ICh1 ! (x: Int) =>
        val r = x + 21
        println("result: "+r)
      case ICh2 ! y =>
        println("received: "+y)
    }
  }

  val b = actor {
    react {
      case Msg(ch1, ch2) => ch1 ! 21
    }
  }
}
