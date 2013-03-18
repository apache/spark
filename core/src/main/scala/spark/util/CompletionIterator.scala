package spark.util

/**
 * Wrapper around an iterator which calls a completion method after it successfully iterates through all the elements
 */
abstract class CompletionIterator[+A, +I <: Iterator[A]](sub: I) extends Iterator[A]{
  def next = sub.next
  def hasNext = {
    val r = sub.hasNext
    if (!r) {
      completion
    }
    r
  }

  def completion()
}

object CompletionIterator {
  def apply[A, I <: Iterator[A]](sub: I, completionFunction: => Unit) : CompletionIterator[A,I] = {
    new CompletionIterator[A,I](sub) {
      def completion() = completionFunction
    }
  }
}