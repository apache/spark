package spark.util

/**
 * Wrapper around an iterator which calls a cleanup method when its finished iterating through its elements
 */
abstract class CleanupIterator[+A, +I <: Iterator[A]](sub: I) extends Iterator[A]{
  def next = sub.next
  def hasNext = {
    val r = sub.hasNext
    if (!r) {
      cleanup
    }
    r
  }

  def cleanup
}

object CleanupIterator {
  def apply[A, I <: Iterator[A]](sub: I, cleanupFunction: => Unit) : CleanupIterator[A,I] = {
    new CleanupIterator[A,I](sub) {
      def cleanup = cleanupFunction
    }
  }
}