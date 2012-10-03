package spark.api.java.function

import scala.runtime.AbstractFunction2

/**
 * Subclass of Function2 for ease of calling from Java. The main thing it does is re-expose the
 * apply() method as call() and declare that it can throw Exception (since AbstractFunction2.apply
 * isn't marked to allow that).
 */
private[spark] abstract class WrappedFunction2[T1, T2, R] extends AbstractFunction2[T1, T2, R] {
  @throws(classOf[Exception])
  def call(t1: T1, t2: T2): R

  final def apply(t1: T1, t2: T2): R = call(t1, t2)
}
