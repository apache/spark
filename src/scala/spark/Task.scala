package spark

import mesos._

@serializable
trait Task[T] {
  def run: T
  def preferredLocations: Seq[String] = Nil
  def markStarted(offer: SlaveOffer) {}
}

@serializable
class FunctionTask[T](body: () => T)
extends Task[T] {
  def run: T = body()
}
