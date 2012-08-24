package spark.broadcast

import java.io._
import java.net._
import java.util.{BitSet, UUID}
import java.util.concurrent.{Executors, ThreadFactory, ThreadPoolExecutor}

import scala.collection.mutable.Map

import spark._

trait Broadcast[T] extends Serializable {
  val uuid = UUID.randomUUID

  def value: T

  // We cannot have an abstract readObject here due to some weird issues with
  // readObject having to be 'private' in sub-classes.

  override def toString = "spark.Broadcast(" + uuid + ")"
}

class BroadcastManager(val isMaster_ : Boolean) extends Logging with Serializable {

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
        broadcastFactory.initialize(isMaster)

        initialized = true
      }
    }
  }

  def stop() {
    broadcastFactory.stop()
  }

  private def getBroadcastFactory: BroadcastFactory = {
    if (broadcastFactory == null) {
      throw new SparkException ("Broadcast.getBroadcastFactory called before initialize")
    }
    broadcastFactory
  }

  def newBroadcast[T](value_ : T, isLocal: Boolean) = broadcastFactory.newBroadcast[T](value_, isLocal)

  def isMaster = isMaster_
}
