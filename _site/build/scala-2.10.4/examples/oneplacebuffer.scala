package examples

object onePlaceBuffer {

  import scala.actors.Actor._
  import scala.concurrent._

  class OnePlaceBuffer {
    private case class Put(x: Int)
    private case object Get
    private case object Stop

    private val m = actor {
      var buf: Option[Int] = None
      loop {
        react {
          case Put(x) if buf.isEmpty =>
            println("put "+x) 
            buf = Some(x)
            reply()
          case Get if !buf.isEmpty =>
            val x = buf.get
            println("get "+x)
            buf = None
            reply(x)
          case Stop => exit()
        }
      }
    }

    def write(x: Int) { m !? Put(x) }

    def read(): Int = (m !? Get).asInstanceOf[Int]

    def finish() { m ! Stop }
  }

  def main(args: Array[String]) {
    scala.actors.Debug.level = 5
    val buf = new OnePlaceBuffer
    val random = new java.util.Random()
    val MaxWait = 500L
    val awhile = 10000L
    @volatile var isDone = false
    def finish = isDone = true
    def finishAfter(delay: Long) = new java.util.Timer(true).schedule(
      new java.util.TimerTask {
        override def run() { finish }
      },
      delay) // in milliseconds
    def sleepTight(length: Int = random nextInt 1000) = {
      var ok = true
      try Thread.sleep(length)
      catch { case e: InterruptedException => ok = false }
      ok
    }
    def producer(n: Int) {
      if (isDone || !sleepTight()) { buf write -1; return }
      buf write n
      producer(n + 1)
    }
    def consumer {
      sleepTight()
      val n = buf.read()
      if (n < 0) return
      consumer
    }
    val maker = future { producer(0) }
    val taker = future { consumer }
    taker onComplete { _ => buf.finish() }
    finishAfter(awhile)
  }
}
