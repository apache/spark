package examples.actors

import scala.actors.Actor._
import scala.reflect.ClassTag

object boundedbuffer {
  class BoundedBuffer[T](N: Int)(implicit m: ClassTag[T]) {
    private case class Put(x: T)
    private case object Get
    private case object Stop

    private val buffer = actor {
      val buf = new Array[T](N)
      var in, out, n = 0
      loop {
        react {
          case Put(x) if n < N =>
            buf(in) = x; in = (in + 1) % N; n += 1; reply()
          case Get if n > 0 =>
            val r = buf(out); out = (out + 1) % N; n -= 1; reply(r)
          case Stop =>
            reply(); exit("stopped")
        }
      }
    }

    def put(x: T) { buffer !? Put(x) }
    def get: T = (buffer !? Get).asInstanceOf[T]
    def stop() { buffer !? Stop }
  }

  def main(args: Array[String]) {
    val buf = new BoundedBuffer[Int](1)
    buf.put(42)
    println("" + buf.get)
    buf.stop()
  }
}
