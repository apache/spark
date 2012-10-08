package spark.api.java.function

import scala.runtime.AbstractFunction1

/**
 * Subclass of Function1 for ease of calling from Java. The main thing it does is re-expose the
 * apply() method as call() and declare that it can throw Exception (since AbstractFunction1.apply
 * isn't marked to allow that).
 */
private[spark] abstract class WrappedFunction1[T, R] extends AbstractFunction1[T, R] {
  @throws(classOf[Exception])
  def call(t: T): R

  final def apply(t: T): R = call(t)
}
