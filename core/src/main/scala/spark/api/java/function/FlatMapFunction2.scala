package spark.api.java.function

/**
 * A function that takes two inputs and returns zero or more output records.
 */
abstract class FlatMapFunction2[A, B, C] extends Function2[A, B, java.lang.Iterable[C]] {
  @throws(classOf[Exception])
  def call(a: A, b:B) : java.lang.Iterable[C]

  def elementType() : ClassManifest[C] = ClassManifest.Any.asInstanceOf[ClassManifest[C]]
}
