package examples

object boundedBuffer {
  import scala.concurrent.{ future, Future, promise }
  import scala.reflect.ClassTag
  import scala.util.{ Try, Success, Failure }
  import java.util.concurrent.{ CountDownLatch, ExecutionException }

  class BoundedBuffer[A](N: Int)(implicit m: ClassTag[A]) {
    var in, out, n = 0
    val elems = new Array[A](N)

    def await(cond: => Boolean) = while (!cond) { wait() }

    def put(x: A) = synchronized {
      await (n < N)
      elems(in) = x; in = (in + 1) % N; n += 1
      if (n == 1) notifyAll()
    }

    def get: A = synchronized {
      await (n != 0)
      val x = elems(out); out = (out + 1) % N; n -= 1
      if (n == N - 1) notifyAll()
      x
    }
  }

  @inline implicit class Fortuna(val s: String) extends AnyVal {
    def isUnlucky = s match {
      case "13" => true
      case _ => false
    }
  }

  def main(args: Array[String]) {
    val buf = new BoundedBuffer[String](5)
    val Halt = "halt"
    val max = (Try(args.head.toInt) filter (_ > 0) recover { case _ => 10 }).get
    val maker = future {
      def produceString(i: Int) = i.toString
      for (i <- 1 to max) buf put produceString(i)
      buf put Halt
    }
    val taker = future {
      import collection.mutable.ListBuffer
      require(max % 2 == 0, s"Max must be positive and even: $max")
      val res = ListBuffer[String]()
      def consumeString(s: String) = res += s
      var done = false
      while (!done) {
        val s = buf.get ensuring (!_.isUnlucky)
        if (s == Halt) done = true
        else consumeString(s)
      }
      res.toList
    }
    val done = new CountDownLatch(1)
    taker onComplete {
      case _ => done.countDown
    }
    done.await()
    taker.value.get match {
      case Success(msgs) => msgs foreach println
      case Failure(e: ExecutionException) => println("Execution failed!"); e.getCause().printStackTrace()
      case Failure(e) => e.printStackTrace()
    }
  }
}
