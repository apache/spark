package spark.broadcast

import java.io._
import java.util.concurrent.atomic.AtomicLong

import spark._

abstract class Broadcast[T](private[spark] val id: Long) extends Serializable {
  def value: T

  // We cannot have an abstract readObject here due to some weird issues with
  // readObject having to be 'private' in sub-classes.

  override def toString = "spark.Broadcast(" + id + ")"
}

private[spark] 
class BroadcastManager(val _isDriver: Boolean) extends Logging with Serializable {

  private var initialized = false
  private var broadcastFactory: BroadcastFactory = null

  initialize()

  // Called by SparkContext or Executor before using Broadcast
  private def initialize() {
    synchronized {
      if (!initialized) {
        val broadcastFactoryClass = System.getProperty(
          "spark.broadcast.factory", "spark.broadcast.HttpBroadcastFactory")

        broadcastFactory =
          Class.forName(broadcastFactoryClass).newInstance.asInstanceOf[BroadcastFactory]

        // Initialize appropriate BroadcastFactory and BroadcastObject
        broadcastFactory.initialize(isDriver)

        initialized = true
      }
    }
  }

  def stop() {
    broadcastFactory.stop()
  }

  private val nextBroadcastId = new AtomicLong(0)

  def newBroadcast[T](value_ : T, isLocal: Boolean) =
    broadcastFactory.newBroadcast[T](value_, isLocal, nextBroadcastId.getAndIncrement())

  def isDriver = _isDriver
}
