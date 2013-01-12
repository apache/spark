package spark.util

import org.scalatest.FunSuite
import java.io.ByteArrayOutputStream
import java.util.concurrent.TimeUnit._

class RateLimitedOutputStreamSuite extends FunSuite {

  private def benchmark[U](f: => U): Long = {
    val start = System.nanoTime
    f
    System.nanoTime - start
  }

  test("write") {
    val underlying = new ByteArrayOutputStream
    val data = "X" * 41000
    val stream = new RateLimitedOutputStream(underlying, 10000)
    val elapsedNs = benchmark { stream.write(data.getBytes("UTF-8")) }
    assert(SECONDS.convert(elapsedNs, NANOSECONDS) == 4)
    assert(underlying.toString("UTF-8") == data)
  }
}
