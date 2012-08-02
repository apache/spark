package spark.api.java.function

abstract class FlatMapFunction[T, R] extends Function[T, java.lang.Iterable[R]] {
  def apply(x: T) : java.lang.Iterable[R]

  def elementType() : ClassManifest[R] = ClassManifest.Any.asInstanceOf[ClassManifest[R]]
}
