package spark.util

import java.util.concurrent.atomic.AtomicInteger

/**
 * A util used to get a unique generation ID. This is a wrapper around Java's
 * AtomicInteger. An example usage is in BlockManager, where each BlockManager
 * instance would start an Akka actor and we use this utility to assign the Akka
 * actors unique names.
 */
private[spark] class IdGenerator {
  private var id = new AtomicInteger
  def next: Int = id.incrementAndGet
}
