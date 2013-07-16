package spark.api.java.function

import scala.reflect.ClassTag

/**
 * A function that takes two inputs and returns zero or more output records.
 */
abstract class FlatMapFunction2[A, B, C] extends Function2[A, B, java.lang.Iterable[C]] {
  @throws(classOf[Exception])
  def call(a: A, b:B) : java.lang.Iterable[C]

  def elementType() : ClassTag[C] = ClassTag.Any.asInstanceOf[ClassTag[C]]
}
