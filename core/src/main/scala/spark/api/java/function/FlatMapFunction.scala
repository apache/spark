package spark.api.java.function

import scala.reflect.ClassTag

/**
 * A function that returns zero or more output records from each input record.
 */
abstract class FlatMapFunction[T, R] extends Function[T, java.lang.Iterable[R]] {
  @throws(classOf[Exception])
  def call(x: T) : java.lang.Iterable[R]

  def elementType() : ClassTag[R] = ClassTag.Any.asInstanceOf[ClassTag[R]]
}
