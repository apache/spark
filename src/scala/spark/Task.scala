package spark

import nexus._

@serializable
trait Task[T] {
  def run: T
  def prefers(slot: SlaveOffer): Boolean = true
  def markStarted(slot: SlaveOffer) {}
}

@serializable
class FunctionTask[T](body: () => T)
extends Task[T] {
  def run: T = body()
}
