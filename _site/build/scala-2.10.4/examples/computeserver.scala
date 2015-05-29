package examples

import language.implicitConversions

import scala.concurrent.{ Channel, ExecutionContext, future, Future, promise }
import scala.concurrent.util.Duration
import scala.util.{ Try, Success, Failure }
import java.util.concurrent.{ CountDownLatch, Executors }
import java.util.concurrent.atomic._

object computeServer {

  type Stats = Tuple3[Int,Int,Int]

  class ComputeServer(n: Int, completer: Try[Stats] => Unit)(implicit ctx: ExecutionContext) {

    private trait Job {
      type T
      def task: T
      def complete(x: Try[T]): Unit
    }

    private val openJobs = new Channel[Job]()

    private def processor(i: Int) = {
      printf("processor %d starting\n", i)
      // simulate failure in faulty #3
      if (i == 3) throw new IllegalStateException("processor %d: Drat!" format i)
      var good = 0
      var bad = 0
      while (!isDone) {
        val job = openJobs.read
        printf("processor %d read a job\n", i)
        val res = Try(job.task)
        if (res.isSuccess) good += 1
        else bad += 1
        job complete res
      }
      printf("processor %d terminating\n", i)
      (i, good, bad)
    }

    def submit[A](body: => A): Future[A] = {
      val p = promise[A]()
      openJobs.write {
        new Job {
          type T = A
          def task = body
          def complete(x: Try[A]) = p complete x
        }
      }
      p.future
    }

    val done = new AtomicBoolean
    def isDone = done.get
    def finish() {
      done set true
      val nilJob =
        new Job {
          type T = Null
          def task = null
          def complete(x: Try[Null]) { }
        }
      // unblock readers
      for (_ <- 1 to n) { openJobs write nilJob }
    }

    // You can, too! http://www.manning.com/suereth/
    def futured[A,B](f: A => B): A => Future[B] = { in => future(f(in)) }
    def futureHasArrived(f: Future[Stats]) = f onComplete completer

    1 to n map futured(processor) foreach futureHasArrived
  }

  @inline implicit class Whiling(val latch: CountDownLatch) extends AnyVal {
    def awaitAwhile()(implicit d: Duration): Boolean = latch.await(d.length, d.unit)
  }

  def main(args: Array[String]) {
    def usage(msg: String = "scala examples.computeServer <n>"): Nothing = {
      println(msg)
      sys.exit(1)
    }
    if (args.length > 1) usage()
    val rt = Runtime.getRuntime
    import rt.{ availableProcessors => avail }
    def using(n: Int) = { println(s"Using $n processors"); n }
    val numProcessors = (Try(args.head.toInt) filter (_ > 0) map (_ min avail) recover {
      case _: NumberFormatException => usage()
      case _ => using(4 min avail)
    }).get

    implicit val ctx = ExecutionContext fromExecutorService (Executors newFixedThreadPool (numProcessors))
    val doneLatch = new CountDownLatch(numProcessors)
    def completer(e: Try[Stats]) {
      e match {
        case Success(s) => println(s"Processor ${s._1} completed ${s._2} jobs with ${s._3} errors")
        case Failure(t) => println("Processor terminated in error: "+ t.getMessage)
      }
      doneLatch.countDown()
    }
    val server = new ComputeServer(numProcessors, completer _)

    val numResults = 3
    val resultLatch = new CountDownLatch(numResults)
    class ResultCounter[A](future: Future[A]) {
      def onResult[B](body: PartialFunction[Try[A], B])(implicit x: ExecutionContext) =
        future andThen body andThen { case _ => resultLatch.countDown() }
    }
    implicit def countingFuture[A](f: Future[A]): ResultCounter[A] = new ResultCounter[A](f)

    def dbz = 1/0
    val k = server submit dbz
    k onResult {
      case Success(v) => println("k returned? "+ v)
      case Failure(e) => println("k failed! "+ e)
    }

    val f = server submit 42
    val g = server submit 38
    val h = for (x <- f; y <- g) yield { x + y }
    h onResult { case Success(v) => println(s"Computed $v") }

    val report: PartialFunction[Try[_], Unit] = {
      case Success(v) => println(s"Computed $v")
      case Failure(e) => println(s"Does not compute: $e")
    }
    val r =
      for {
        x <- server submit 17
        y <- server submit { throw new RuntimeException("Simulated failure"); 13 }
      } yield (x * y)
    r onResult report

    implicit val awhile = Duration("1 sec")
    def windDown() = {
      server.finish()
      doneLatch.awaitAwhile()
    }
    def shutdown() = {
      ctx.shutdown()
      ctx.awaitTermination(awhile.length, awhile.unit)
    }
    val done = resultLatch.awaitAwhile() && windDown() && shutdown()
    assert(done, "Error shutting down.")
  }
}
