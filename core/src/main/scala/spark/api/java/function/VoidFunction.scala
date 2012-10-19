package spark.api.java.function

/**
 * A function with no return value.
 */
// This allows Java users to write void methods without having to return Unit.
abstract class VoidFunction[T] extends Serializable {
  @throws(classOf[Exception])
  def call(t: T) : Unit
}

// VoidFunction cannot extend AbstractFunction1 (because that would force users to explicitly
// return Unit), so it is implicitly converted to a Function1[T, Unit]:
object VoidFunction {
  implicit def toFunction[T](f: VoidFunction[T]) : Function1[T, Unit] = ((x : T) => f.call(x))
}