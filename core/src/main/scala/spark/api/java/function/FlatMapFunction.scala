package spark.api.java.function

/**
 * A function that returns zero or more output records from each input record.
 */
abstract class FlatMapFunction[T, R] extends Function[T, java.lang.Iterable[R]] {
  @throws(classOf[Exception])
  def call(x: T) : java.lang.Iterable[R]

  def elementType() : ClassManifest[R] = ClassManifest.Any.asInstanceOf[ClassManifest[R]]
}
