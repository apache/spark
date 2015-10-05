package examples.actors

import scala.actors.{Actor, Scheduler}
import scala.actors.Actor._
import scala.actors.scheduler.SingleThreadedScheduler

object message {
  def main(args: Array[String]) {
    val n = try { args(0).toInt }
    catch {
      case _: Throwable =>
        println("Usage: examples.actors.message <n>")
        sys.exit
    }
    val nActors = 500
    val finalSum = n * nActors
    Scheduler.impl = new SingleThreadedScheduler

    def beh(next: Actor, sum: Int) {
      react {
        case value: Int =>
          val j = value + 1; val nsum = sum + j
          if (next == null && nsum >= n * j)
            println(nsum)
          else {
            if (next != null) next ! j
            if (nsum < n * j) beh(next, nsum)
          }
      }
    }

    def actorChain(i: Int, a: Actor): Actor =
      if (i > 0) actorChain(i-1, actor(beh(a, 0))) else a

    val firstActor = actorChain(nActors, null)
    var i = n; while (i > 0) { firstActor ! 0; i -= 1 }

    Scheduler.shutdown()
  }
}
