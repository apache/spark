package spark

@serializable
abstract class Task[T] {
  def run: T
  def preferredLocations: Seq[String] = Nil
  def generation: Option[Long] = None
}
