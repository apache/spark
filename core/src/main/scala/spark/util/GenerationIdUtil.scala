package spark.util

import java.util.concurrent.atomic.AtomicInteger

private[spark]
object GenerationIdUtil {

  val BLOCK_MANAGER = new IdGenerator

  /**
   * A util used to get a unique generation ID. This is a wrapper around
   * Java's AtomicInteger.
   */
  class IdGenerator {
    private var id = new AtomicInteger

    def next: Int = id.incrementAndGet
  }
}
